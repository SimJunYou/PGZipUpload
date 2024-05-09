import os
import sys
import subprocess
import logging
import typing
from pathlib import Path
from zipfile import ZipFile
from shutil import rmtree
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from tqdm import tqdm
import traceback
# from functools import partial
# from tqdm.contrib.concurrent import process_map, thread_map

from sqlalchemy import create_engine, text

from utils import CodeTimer, setup_logging_in_process, setup_logging_in_thread, COPY_CSV_SQL

ZipMethod = typing.Literal["7z", "py", "async"]  # enum for unzipping methods


class UploadWorker:
    def __init__(
            self,
            zip_path: str,  # e.g. "E:\\2022\01"
            temp_path: str,  # e.g. "D:\\CapstoneTemp"
            is_dedicated_temp: bool = False,
            does_direct_upload: bool = False,
            db_conn_str: str = None,
            zip_method: ZipMethod = "py",
            name: str = None
    ):
        self.zip_path = Path(zip_path)  # where all zip files are located
        self.temp_path = Path(temp_path)  # temp directory to store files
        self.zip_list = os.listdir(self.zip_path)
        self.zip_method = zip_method

        # for direct DB uploading
        self.db_conn_str = db_conn_str
        self.does_direct_upload = does_direct_upload

        # logger vars
        self.name = self.zip_path.name if name is None else name
        self.logger = logging.getLogger(f"UploadWorker[{self.name}]")
        self.configure_logger()

        # sanity check on params
        if is_dedicated_temp:
            self.reset_temps()
        if not is_dedicated_temp and len(self.get_temps()) > 0:
            raise FileExistsError("Non-dedicated temp directory is not empty! Aborting...")
        if self.does_direct_upload and self.zip_method != "async":
            raise NotImplementedError("Direct upload only works with the 'async' unzip method.")
        if self.does_direct_upload and self.db_conn_str is None:
            raise NotImplementedError("Database connection string is required if performing direct upload.")


    def configure_logger(self):
        self.logger.setLevel(logging.DEBUG)  # logger's base level should catch everything

        file_handler = logging.FileHandler(f'../logs/worker_{self.name}.log', mode='w+')
        file_handler.setLevel(logging.DEBUG)  # file logs are to assist with debugging, just log everything
        file_handler.setFormatter(
            logging.Formatter(f'%(asctime)s - %(levelname)s - UploadWorker ({self.name}): %(message)s',
                              datefmt='%Y-%m-%d %H:%M:%S')
        )
        self.logger.addHandler(file_handler)

        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setLevel(logging.WARNING)  # keep the terminal output nice and neat (or just disable completely)
        stream_handler.setFormatter(
            logging.Formatter(f'UploadWorker ({self.name}): %(levelname)s - %(message)s')
        )
        self.logger.addHandler(stream_handler)

    def check_health(self) -> None:
        # Debug function, used to verify the health of unzipped contents in the temp dir
        temps = self.get_temps()
        if len(temps) == 0:
            print("No temp files found!")
            return

        alert_threshold = 1_000_000  # 1MB
        file_sizes = [os.path.getsize(os.path.join(self.temp_path, tempf)) for tempf in temps]
        bad_file_exists = False
        for tempf, tempf_size in zip(temps, file_sizes):
            if tempf_size <= alert_threshold:
                bad_file_exists = True
                print(f"Bad file {tempf}: {tempf_size} bytes")

        if not bad_file_exists:
            print("All extracted files healthy!")

    def get_temps(self) -> list:
        if not os.path.exists(self.temp_path):
            return []
        return os.listdir(self.temp_path)

    def reset_temps(self) -> None:
        if not os.path.exists(self.temp_path):
            self.logger.debug(f"Temp directory not found at {self.temp_path}, making it now")
            os.makedirs(self.temp_path)  # make dir if it doesn't exist
        if len(self.get_temps()) > 0:
            self.logger.debug("Resetting temp directory")
            rmtree(self.temp_path, ignore_errors=True)  # empty out the temp extract path first
        else:
            self.logger.debug("Temp directory already empty")

    def unzip_file(self, file_idx: int) -> None:
        if not self.does_direct_upload:
            self.reset_temps()

        file_name: str = self.zip_list[file_idx]
        try:
            if self.zip_method == "7z":
                self.logger.info("Unzipping file with 7zip")
                self.unzip_method_7z(file_name)
            elif self.zip_method == "py":
                self.logger.info("Unzipping file with Python's ZipFile")
                self.unzip_method_py(file_name)
            elif self.zip_method == "async":
                self.logger.info("Unzipping file asynchronously")
                if self.does_direct_upload:
                    self.logger.info("Directly uploading to DB as well")
                self.unzip_method_async(file_name)
            self.update_progress(file_idx)
        except Exception as e:
            full_error_msg = ''.join(traceback.format_tb(e.__traceback__)) + '\n' + str(e)
            self.update_progress(file_idx, full_error_msg)

    def update_progress(self, file_idx: int, error_msg: str = None) -> None:
        if error_msg is None:
            log_file_name = "../logs/completed_files.log"
        else:
            log_file_name = "../logs/failed_files.log"
        with open(log_file_name, 'a+') as log_file:
            print(self.zip_list[file_idx], file=log_file)
            if error_msg is not None:
                print(error_msg, file=log_file)

    def unzip_method_7z(self, file_name: str) -> None:
        file_path: Path = self.zip_path / file_name
        subprocess.run(
            ["7z", "e", str(file_path), "-o" + str(self.temp_path)],
            capture_output=True
        )

    def unzip_method_py(self, file_name: str) -> None:
        file_path: Path = self.zip_path / file_name
        ZipFile(file_path).extractall(path=self.temp_path)

    def unzip_method_async(self, file_name: str) -> None:
        file_path: Path = self.zip_path / file_name
        with ZipFile(file_path, 'r') as handle:
            content_list: list[str] = handle.namelist()

        # unzip the zip file in chunks, handled by async workers with one process each
        n_workers = 8
        n_threads = 10
        chunk_size = (len(content_list) // n_workers) + 1
        self.logger.info(f"Number of files: {len(content_list)}")
        self.logger.info(f"Chunk size: {chunk_size}")
        self.logger.info(f"Direct upload: {self.does_direct_upload}")

        futures = []
        if self.does_direct_upload:
            with ProcessPoolExecutor(max_workers=n_workers) as process_pool:
                for i in range(0, len(content_list), chunk_size):
                    content_chunk = content_list[i:(i + chunk_size)]
                    task_future = process_pool.submit(
                        _unzip_files_direct,
                        self.db_conn_str,
                        file_path,
                        content_chunk,
                        i // chunk_size
                    )
                    futures.append(task_future)
        else:
            with ProcessPoolExecutor(max_workers=n_workers) as process_pool:
                for i in range(0, len(content_list), chunk_size):
                    content_chunk = content_list[i:(i + chunk_size)]
                    task_future = process_pool.submit(
                        _unzip_files,
                        n_threads,
                        file_path,
                        self.temp_path,
                        content_chunk,
                        i // chunk_size
                    )
                    futures.append(task_future)
        for i, future in enumerate(futures):
            future.result()  # will raise exception if the job failed
        self.logger.info(f"All chunks successfully completed")


def _unzip_files(n_threads: int, zip_filepath: Path, temp_path: Path, content_names: list[str], chunk_idx: int):
    # Helper method for async unzipping
    # Use threads here since we're using processes outside - 10 threads per process
    # Each thread handles the reading and saving of a single file in the zip
    # Defined on top level since multiprocessing can only pickle top level functions
    logger = setup_logging_in_process(chunk_idx)
    logger.info(f"Process started")
    futures = []
    with ZipFile(zip_filepath, 'r') as zf, ThreadPoolExecutor(max_workers=n_threads) as thread_pool:
        for content_name in content_names:
            data = zf.read(content_name)  # decompress data
            logger.info(f"Submitting file {content_name} to thread worker")
            futures.append(
                thread_pool.submit(_save_file, data, temp_path, content_name)  # save to disk (async)
            )
    for i, future in enumerate(futures):
        future.result()  # will raise exception if the job failed
    logger.info(f"All files successfully uploaded")


def _unzip_files_direct(conn_str: str, zip_filepath: Path, content_names: list[str], chunk_idx: int):
    # Direct version of _unzip_files
    logger = setup_logging_in_process(chunk_idx)
    logger.info(f"Process started")
    proc_engine = create_engine(conn_str)  # require one engine for each process
    proc_conn = proc_engine.raw_connection()
    # futures = []
    with ZipFile(zip_filepath, 'r') as zf:
        for content_name in content_names:
            logger.info(f"Extracting and uploading {content_name}")
            with zf.open(content_name) as data:
                proc_conn.cursor().copy_expert(COPY_CSV_SQL, data)
        proc_conn.commit()
    proc_conn.close()
    logger.info(f"All files successfully uploaded")


def _copy_data_to_db(engine, data: typing.IO[bytes]):
    # Helper method for async uploading direct to DB
    # Defined on top level since multiprocessing can only pickle top level functions
    with engine.raw_connection() as conn:
        conn.cursor().copy_expert(COPY_CSV_SQL, data)
        conn.commit()


def _save_file(data: bytes, temp_path: Path, output_name: str):
    # Helper method for async saving files
    # Defined on top level since multiprocessing can only pickle top level functions

    output_path = os.path.join(temp_path, output_name)
    with open(output_path, 'wb') as file:
        file.write(data)


if __name__ == "__main__":
    curr_zip_path = "E:\\2022\\01"
    curr_temp_path = "D:\\CapstoneTemp"
    DEFAULT_CONN_STR = "postgresql://JunYou:@localhost:5432/postgres"
    with open('sql/create_staging_table.sql', 'r') as f:
        CREATE_TABLE_SQL = f.read()
        print(CREATE_TABLE_SQL)
    with create_engine(DEFAULT_CONN_STR).connect() as testconn:
        print("Creating table")
        testconn.execute(text("DROP TABLE IF EXISTS ais_staging;"))
        testconn.execute(text(CREATE_TABLE_SQL))
        testconn.commit()
        res = testconn.execute(text("SELECT * FROM ais_staging LIMIT 5;"))  # will throw error if table does not exist
        print(res.all())

    with CodeTimer('Async'):
        zw_as = UploadWorker(
            curr_zip_path,
            curr_temp_path,
            db_conn_str=DEFAULT_CONN_STR,
            does_direct_upload=True,
            is_dedicated_temp=True,
            zip_method="async",
            name="2022_01",
        )
        for i in tqdm(range(2)):  # 2 days
            zw_as.unzip_file(i)
