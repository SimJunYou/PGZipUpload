from pathlib import Path
from zipfile import ZipFile
import os

from sqlalchemy import create_engine
from utils import CodeTimer
import io

eng = create_engine("postgresql://JunYou:@localhost:5432/postgres")
conn = eng.raw_connection()
with CodeTimer("Separate Files"):
    for i in range(1, 201):
        with open(f"testfiles/test{i}.csv", "r") as f:
            conn.cursor().copy_expert("COPY ais_unlogged1 FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", f)
            conn.commit()
conn.close()

conn2 = eng.raw_connection()
with CodeTimer("Combined Files"):
    allfiles = io.StringIO()
    for i in range(1, 201):
        with open(f"testfiles/test{i}.csv", "r") as f:
            if i != 1:
                f.readline()  # skip header if not first file
            allfiles.write(f.read())
    allfiles.seek(0)
    conn2.cursor().copy_expert("COPY ais_unlogged2 FROM STDIN WITH (FORMAT CSV, HEADER TRUE)", allfiles)
    conn2.commit()
conn2.close()