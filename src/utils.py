import timeit
import logging
import threading
import warnings

warnings.filterwarnings("ignore")

CONN_STR = "postgresql://JunYou:@localhost:5432/postgres"

with open('sql/create_staging_table.sql', 'r') as f:
    CREATE_TABLE_SQL = f.read()

with open('sql/copy_from_stdin.sql', 'r') as f:
    COPY_CSV_SQL = f.read()


def setup_logging_in_process(process_id):
    # process_id is not necessarily the actual process's ID
    # Set up logging configuration
    logger = logging.getLogger(f'Process {process_id}')
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(f'%(asctime)s - %(levelname)s - Process {process_id}: %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    # Create file handler
    file_handler = logging.FileHandler(f'../logs/process_{process_id}.log', mode='w+')
    file_handler.setFormatter(formatter)

    # Add file handler to logger
    logger.addHandler(file_handler)

    return logger


def setup_logging_in_thread():
    # Get the current thread name
    thread_name = threading.current_thread().name

    # Set up logging configuration
    logger = logging.getLogger(thread_name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s: %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')

    # Create file handler
    file_handler = logging.FileHandler('thread_{}.log'.format(thread_name), mode='w')
    file_handler.setFormatter(formatter)

    # Add file handler to logger
    logger.addHandler(file_handler)

    return logger


class CodeTimer:
    def __init__(self, name=None):
        self.name = " '" + name + "'" if name else ''

    def __enter__(self):
        self.start = timeit.default_timer()

    def __exit__(self, exc_type, exc_value, traceback):
        duration = (timeit.default_timer() - self.start) * 1000.0
        if duration < 1000:
            unit = "ms"
        else:
            duration /= 1000
            unit = "s"
        print(f"{self.name} took: {duration:.3f} {unit}")

# zip_tqdm = tqdm(all_zips)
# for zipf in zip_tqdm:
#     zipdate = zipf[11:-4]
#     zip_tqdm.set_description(f"[{zipdate}] Unzipping")
#     csv_list = unzip_file(zipf)
#     for i, csvf in enumerate(csv_list):
#         zip_tqdm.set_description(f"[{zipdate}] Uploading [{i}/{len(csv_list)}]")
#         with open(os.path.join(temp_extract_path, csvf), 'r') as f:
#            cursor.copy_expert(copy_csv_sql, f)
#            conn.commit()
#     zip_tqdm.set_description(f"[{zipdate}] Clearing temp files")
#     rmtree(temp_extract_path)
# print("Done!")
