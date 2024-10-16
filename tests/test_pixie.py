# Function that starts a clock, runs a function, and returns the elapsed time. Used to measure parallel performance.
import time
from datetime import datetime

import pandas as pd

from parallelpixie.pixie import Pixie
from parallelpixie.processors import replace_data, generate_chunked_plot, transform_column_data, clean_rows

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
    temp = Pixie.from_sqlite('../covid-data.db', "covid_data")
    generate_chunked_plot(temp.data_source, 7, 9, plot_kwargs, label_kwargs)
    return temp.data_source

def format_date(date_string, date_format):
    if(date_string == '9999-99-99'):
        return date_string
    result = datetime.strptime(date_string, date_format)
    formattedResult = datetime.strftime(result, '%Y-%m-%d')
    return formattedResult

def test_transform_column():
    temp = Pixie.from_csv('../sample-covid-data.csv')
    print('finished reading data')
    date_format = '%d/%m/%Y'
    print('calling transform')
    results = transform_column_data('DATE_DIED', temp.data_source, format_date, [date_format])

    for chunk in results:
        print(chunk.columns)
        print(chunk['DATE_DIED'])
    return results

def test_clean_rows():
    temp = Pixie.from_csv('../sample-covid-data.csv')
    results = clean_rows(97, temp.data_source, 'PREGNANT')
    for chunk in results:
        print(chunk.columns)
        print(chunk['PREGNANT'])
    return results


if __name__ == '__main__':
    # sample_df = pd.read_csv("../covid-data.csv", nrows=1000)
    # avg_row_size = sample_df.memory_usage(deep=True).sum() / len(sample_df)
    #print(f"Average row size: {avg_row_size} bytes")

    test_sqlite()

    processing_time = (time.time() - start) / 60
    print(f"Total processing time: {processing_time} minutes")