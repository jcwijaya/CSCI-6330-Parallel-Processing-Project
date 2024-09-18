import pandas as pd
from sqlalchemy import create_engine
from parallelpixie.processors import generate_chunked_plot

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
        return cls(pd.read_csv(filename, chunksize=chunks))

    @classmethod
    def from_parquet(cls, filename: str):
        return cls(pd.read_parquet(filename))

    @classmethod
    def from_json(cls, filename: str):
        return cls(pd.read_json(filename))

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
    generate_chunked_plot(temp.data_source, 0, 1, plot_kwargs, label_kwargs)
    #mp.freeze_support()