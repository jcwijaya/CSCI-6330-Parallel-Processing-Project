import pandas as pd
from sqlalchemy import create_engine
from parallelpixie.processors import generate_chunked_plot, replace_data
import ijson
import json
import pyarrow.parquet as pq


# This function is used to find the optimal chunk size for the data source.
# This is NOT FINISHED AND NEEDS REDONE. THIS ONLY WORKS FOR SYSTEMS WITH
# 32 GB OF MEMORY. We need to find the CPU cores and memory available and
# calculate the optimal chunk size from there.
def find_optimal_chunksize():
    # Memory and chunk size calculation
    total_memory = 32000000  # 32 GB
    row_size = 1400000  # Estimate 1 KB per row
    rows_per_chunk = total_memory // row_size  # Adjust based on estimated row size
    chunksize = rows_per_chunk * 10000  # Conservative estimate
    optimal_chunksize = chunksize  # Adjust based on number of cores

    return optimal_chunksize

# Pandas does not natively support chunking parquet files so we'll either need to do it using
# pyarrow or we'll need to find an alternative.
def read_parquet_as_chunks(filename: str):
    pass

# Pandas does not natively support chunking JSON files so we'll either need to do it using
# ijson or we'll need to find an alternative.
def read_json_as_chunks(filename: str):
    pass

# Class template for Pixie
class Pixie:
    def __init__(self, data_source):
        self.data_source = data_source

    #Maybe can query it in chunks by using limit and offset
    @classmethod
    def from_sqlite(cls, db_file: str, table: str):
        connection_string = f"sqlite:///{db_file}"
        engine = create_engine(connection_string)
        query = f"SELECT * FROM {table}"
        return cls(pd.read_sql(query, engine))

    @classmethod
    def from_postgres(cls, host: str, database: str, username: str, password: str, port: int, table: str):
        connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        query = f"SELECT * FROM {table}"
        return cls(pd.read_sql(query, engine))

    @classmethod
    def from_csv(cls, filename: str, chunks: int = find_optimal_chunksize()):
        print(f"Optimal chunksize: {chunks}")
        text_file_reader = pd.read_csv(filename, chunksize=chunks)
        data_chunks = [data_chunk for data_chunk in text_file_reader]
        return cls(data_chunks)

    @classmethod
    def from_parquet(cls, filename: str):
        return cls(pd.read_parquet(filename))

    @classmethod
    def from_json(cls, filename: str):
        return cls(pd.read_json(filename))

    # read large json file (using ijson library)
    @classmethod
    def read_json_as_chunks(cls, filename: str, chunksize: int = find_optimal_chunksize()):
        with open(filename, 'r', encoding='utf-8') as f:
            json_reader = ijson.items(f, '')
            chunk_data = []
            temp_data = []  # temporary storage for the current chunk
            # Initialize an empty list to store JSON objects
            for json_object in json_reader:
                temp_data.append(json_object)
                if len(temp_data) >= chunksize:
                    chunk_data.append(pd.DataFrame(temp_data))
                    temp_data = []
            if temp_data:
                chunk_data.append(pd.DataFrame(temp_data))
        return cls(chunk_data)  # Return an instance of Pixie initialized with the DataFrame

    # read large parquet file by chunks (using pyarrow)
    @classmethod
    def read_parquet_as_chunks(cls, filename: str, chunksize: int = find_optimal_chunksize()):
        print(f"optimal chunksize:{chunksize}")
        parquet_file = pq.ParquetFile(filename)
        # list to store all chunks
        chunk_data = []
        # iterate over the file in chunks
        for batch in parquet_file.iter_batches(batch_size=chunksize):
            df_chunk = batch.to_pandas()
            chunk_data.append(df_chunk)
        return cls(chunk_data)

    def print_table(self):
        print(self.data_source)

# DUMMY TEST LABELS WE NEED TO MAKE IT SO THE END USER CAN CHANGE THESE EASILY
plot_kwargs = {
    'color': 'pink',
    'marker': 'o',
    'linestyle': '-',
    'linewidth': 2,
    'markersize': 8
}

label_kwargs = {
    'title': 'Random Title',
    'xlabel': 'Random X-Axis Label',
    'ylabel': 'Random Y-Axis Label'
}

if __name__ == '__main__':
    temp = Pixie.from_csv('../covid-data.csv')
    for chunk in temp.data_source:
        chunk['ASTHMA'] = chunk['ASTHMA'].astype(str)
        chunk['DIABETES'] = chunk['DIABETES'].astype(str)
    result = replace_data('1', 'Yes', temp.data_source, ['ASTHMA', 'DIABETES'])
    print("finished replacement")

    for chunk in result:
        print(chunk.columns)
        print(chunk['ASTHMA'])
        print(chunk['OBESITY'])

    temp2 = Pixie.read_json_as_chunks('../json-data.json')
    list_of_chunks = temp2.data_source
    for chunk in list_of_chunks:
        print(chunk)

    temp3 = Pixie.read_parquet_as_chunks('../mtcars.parquet')
    print(temp3.data_source)
    generate_chunked_plot(temp.data_source, 0, 1, plot_kwargs, label_kwargs)
    #mp.freeze_support()

