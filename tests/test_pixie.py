# Function that starts a clock, runs a function, and returns the elapsed time. Used to measure parallel performance.
import time

import pandas as pd

from parallelpixie.pixie import Pixie
from parallelpixie.processors import replace_data, generate_chunked_plot

start = time.time()

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

# Test CSV
def test_csv():
    temp = Pixie.from_csv('../covid-data.csv')
    # for chunk in temp.data_source:
    #     chunk['ASTHMA'] = chunk['ASTHMA'].astype(str)
    #     chunk['DIABETES'] = chunk['DIABETES'].astype(str)
    # result = replace_data('1', 'Yes', temp.data_source, ['ASTHMA', 'DIABETES'])
    # print("finished replacement")
    #
    # for chunk in result:
    #     print(chunk.columns)
    #     print(chunk['ASTHMA'])
    #     print(chunk['OBESITY'])
    #
    generate_chunked_plot(temp.data_source, 7, 8, plot_kwargs, label_kwargs)
    return temp.data_source

# Test JSON
def test_json():
    temp = Pixie.from_json('../covid-data.json', 82000)

    generate_chunked_plot(temp.data_source, 7, 8, plot_kwargs, label_kwargs)

# Test Parquet
def test_parquet():
    temp = Pixie.from_parquet('../covid-data.parquet', 82000)
    generate_chunked_plot(temp.data_source, 7, 8, plot_kwargs, label_kwargs)
    return temp.data_source

# Test PostgreSQL

# Test SQLite
def test_sqlite():
    temp = Pixie.from_sqlite('../covid-data.db', "covid-data")
    #generate_chunked_plot(temp.data_source, 7, 8, plot_kwargs, label_kwargs)
    return temp.data_source

if __name__ == '__main__':
    # sample_df = pd.read_csv("../covid-data.csv", nrows=1000)
    # avg_row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    #print(f"Average row size: {avg_row_size} bytes")

    test_csv()

    processing_time = (time.time() - start) / 60
    print(f"Total processing time: {processing_time} minutes")