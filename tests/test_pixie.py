# Function that starts a clock, runs a function, and returns the elapsed time. Used to measure parallel performance.
import array
import time
from datetime import datetime

import pandas as pd

from parallelpixie.pixie import Pixie
from parallelpixie.processors import replace_data, generate_chunked_plot_local, transform_column_data, clean_rows

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
    temp = Pixie.from_csv('../covid-data.csv', 82000)
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
    generate_chunked_plot_local(temp.data_source, 7, 8, plot_kwargs, label_kwargs)
    return temp.data_source

# Test JSON
def test_json():
    temp = Pixie.from_json('../covid-data.json', 82000)

    generate_chunked_plot_local(temp.data_source, 7, 8, plot_kwargs, label_kwargs)

# Test Parquet
def test_parquet():
    temp = Pixie.from_parquet('../covid-data.parquet', 82000)
    generate_chunked_plot_local(temp.data_source, 7, 8, plot_kwargs, label_kwargs)
    return temp.data_source

# Test PostgreSQL

# Test SQLite
def test_sqlite():
    temp = Pixie.from_sqlite('../covid-data.db', "covid_data", 82000)
    generate_chunked_plot_local(temp.data_source, 7, 9, plot_kwargs, label_kwargs)
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
    print("Number of rows in JSON: 3665")
    time_json = array.array('f')
    for i in range(5):
        start = time.time()
        test_json()
        processing_time = (time.time() - start) / 60
        time_json.append(processing_time)
        print(f"Processing time for JSON #{i} {processing_time} minutes")
    print(f"JSON average processing time: {sum(time_json) / len(time_json)} minutes")

    print("Number of rows in Parquet: 124,856")
    time_parquet = array.array('f')
    for i in range(5):
        start = time.time()
        test_parquet()
        processing_time = (time.time() - start) / 60
        time_parquet.append(processing_time)
        print(f"Processing time for Parquet #{i} {processing_time} minutes")
    print(f"Parquet average processing time: {sum(time_parquet) / len(time_parquet)} minutes")

    print("Number of rows in SQLite: 1,048,575")
    time_sqlite = array.array('f')
    for i in range(5):
        start = time.time()
        test_sqlite()
        processing_time = (time.time() - start) / 60
        time_sqlite.append(processing_time)
        print(f"Processing time for SQLite #{i} {processing_time} minutes")
    print(f"SQLite average processing time: {sum(time_sqlite) / len(time_sqlite)} minutes")

    print("Number of rows in CSV: 1,048,575")
    time_csv = array.array('f')
    for i in range(5):
        start = time.time()
        test_csv()
        processing_time = (time.time() - start) / 60
        time_csv.append(processing_time)
        print(f"Processing time for CSV #{i} {processing_time} minutes")
    print(f"CSV average processing time: {sum(time_csv) / len(time_csv)} minutes")

    print("All tests complete")