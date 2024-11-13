import ijson
import pandas as pd
import psutil as ps
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from google.cloud.sql.connector import Connector



# This function is used to find the optimal chunk size for the data source.
# This is NOT FINISHED AND NEEDS REDONE. THIS ONLY WORKS FOR SYSTEMS WITH
# 32 GB OF MEMORY. We need to find the CPU cores and memory available and
# calculate the optimal chunk size from there.
def find_optimal_chunksize():
    total_memory = ps.virtual_memory().available // (1024 ** 2)  # Convert bytes to MB
    num_cores = ps.cpu_count()
    row_size = 1.001  # Estimate 0.05 MB per row for safety
    t = row_size * (total_memory * 0.95) * num_cores
    return max(1, int(t))



# Pandas does not natively support chunking parquet files so we'll either need to do it using
# pyarrow or we'll need to find an alternative.
def read_parquet_as_chunks(filename: str, chunksize: int):
    print(f"optimal chunksize:{chunksize}")
    parquet_file = pq.ParquetFile(filename)

    # Iterate over the file in chunks
    for batch in parquet_file.iter_batches(batch_size=chunksize):
        # Convert the current batch to a pandas DataFrame
        df_chunk = batch.to_pandas()
        # Yield each chunk one at a time
        yield df_chunk



# Pandas does not natively support chunking JSON files so we'll either need to do it using
# ijson or we'll need to find an alternative.
def read_json_as_chunks(filename: str, chunksize: int):
    with open(filename, 'r', encoding='utf-8') as f:
        # Initialize the JSON reader for the array of objects in the JSON
        json_reader = ijson.items(f, 'item')  # Adjust 'item' if needed based on your JSON structure
        temp_data = []  # Temporary storage for the current chunk

        for json_object in json_reader:
            temp_data.append(json_object)
            if len(temp_data) >= chunksize:
                yield pd.DataFrame(pd.json_normalize(temp_data))  # Yield each chunk as a DataFrame
                temp_data = []  # Reset temporary data for the next chunk

        # Yield any remaining data that didn't fill up to `chunksize`
        if temp_data:
            yield pd.DataFrame(pd.json_normalize(temp_data))


            
def get_cloudsql_connection(username, password, database, instance):
    connector = Connector()
    connection = connector.connect(
        instance, #project:region:instance
        "pymysql",
        user=username,
        password=password,
        db=database,)
    return connection

  
  
# Class template for Pixie
class Pixie:
    def __init__(self, data_source):
        self.data_source = data_source
        
    # WORK IN PROGRESS - researching cloudsql connection for sqlalchemy, need to test
    @classmethod
    def from_cloudsql(cls, username, password, database, instance, table):
        connection_string = "mysql+pymysql://"
        engine = create_engine(connection_string, creator=get_cloudsql_connection(username, password, database, instance))
        query = f"SELECT * FROM {table}"
        return cls(pd.read_sql(query, engine))

    @classmethod
    def from_sqlite(cls, db_file: str, table: str, chunks: int = find_optimal_chunksize()):
        connection_string = f"sqlite:///{db_file}"
        engine = create_engine(connection_string)
        query = f"SELECT * FROM {table}"
        return cls(pd.read_sql(query, engine, chunksize=chunks))

    @classmethod
    def from_postgres(cls, host: str, database: str, username: str, password: str, port: int, table: str,
                      chunks: int = find_optimal_chunksize()):
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        query = f"SELECT * FROM {table}"
        return cls(pd.read_sql(query, engine, chunksize=chunks))

    @classmethod
    def from_csv(cls, filename: str, chunks: int = find_optimal_chunksize(), dateColumns: list = None, dateParser = None):
        text_file_reader = pd.read_csv(filename, chunksize=chunks, parse_dates=dateColumns, date_parser=dateParser)
        return cls(text_file_reader)

    @classmethod
    def from_parquet(cls, filename: str, chunks: int = find_optimal_chunksize()):
        return cls(read_parquet_as_chunks(filename, chunks))

    @classmethod
    def from_json(cls, filename: str, chunks: int = find_optimal_chunksize()):
        return cls(read_json_as_chunks(filename, chunks))

    def print_table(self):
        print(self.data_source)
