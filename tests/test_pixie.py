import matplotlib.pyplot as plt
import array
from datetime import datetime

import pandas as pd

from parallelpixie.pixie import Pixie
from parallelpixie.processors import replace_data, generate_plot, transform_column_data, clean_rows

# DUMMY TEST LABELS WE NEED TO MAKE IT SO THE END USER CAN CHANGE THESE EASILY
plot_kwargs = {
    'color': 'pink',
    'marker': 'o',
    'linestyle': '-',
    'linewidth': 2
}

label_kwargs = {
    'title': 'Random Title',
    'xlabel': 'Random X-Axis Label',
    'ylabel': 'Random Y-Axis Label'
}

# Test CSV
def test_csv():
    temp = Pixie.from_csv('../linear_xy_large.csv')
    return generate_plot(temp.data_source, plt.plot, 0, 1, plot_kwargs, label_kwargs, "local", 'linear-data.png', True)

# Test JSON
def test_json():
    temp = Pixie.from_json('../linear_xy_large.json')
    return generate_plot(temp.data_source, plt.plot, 0, 1, plot_kwargs, label_kwargs, "local", 'linear-data.png', True)

# Test Parquet
def test_parquet():
    temp = Pixie.from_parquet('../linear_xy_large.parquet')
    return generate_plot(temp.data_source, plt.plot, 0, 1, plot_kwargs, label_kwargs, "local", 'linear-data.png', True)

# Test SQLite
def test_sqlite():
    temp = Pixie.from_sqlite('../linear_xy_large.db', "xy_data")
    return generate_plot(temp.data_source, plt.plot, 0, 1, plot_kwargs, label_kwargs, "local", 'linear-data.png', True)

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
    csv_time = {}
    json_time = {}
    parquet_time = {}
    sqlite_time = {}

    for x in range(5):
        csv_time[x] = test_csv()
        json_time[x] = test_json()
        parquet_time[x] = test_parquet()
        sqlite_time[x] = test_sqlite()
        print(f"Test {x} complete")

    print("Avg time for CSV:", sum(csv_time.values()) / len(csv_time))
    print("Avg time for PostgreSQL:", sum(csv_time.values()) / len(csv_time))
    print("Avg time for JSON:", sum(json_time.values()) / len(json_time))
    print("Avg time for Parquet:", sum(parquet_time.values()) / len(parquet_time))
    print("Avg time for SQLite:", sum(sqlite_time.values()) / len(sqlite_time))