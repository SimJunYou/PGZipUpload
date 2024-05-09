from upload_worker import UploadWorker


DEFAULT_CONN_STR = "postgresql://JunYou:@localhost:5432/postgres"
with open('sql/create_staging_table.sql', 'r') as f:
    CREATE_TABLE_SQL = f.read()