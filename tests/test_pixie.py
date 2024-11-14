import os
import time

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

from parallelpixie.pixie import Pixie
from parallelpixie.processors import replace_data, generate_plot, transform_column_data, clean_rows, SPINNER, pool_task, \
    process_chunk, thread_task

# Set Matplotlib backend
plt.switch_backend('TkAgg')


# DUMMY TEST LABELS WE NEED TO MAKE IT SO THE END USER CAN CHANGE THESE EASILY
plot_kwargs = {
    'color': 'pink',
    'marker': '.',
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

# Test PostgreSQL
def test_postgresql():
    temp = Pixie.from_postgres('localhost', 'linear_xy_large', 'postgres', 'postgres', 5432, 'xy_data')
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

def test_cloudsql_connection():
    load_dotenv()
    user = os.getenv("DB_USER")
    temp = Pixie.from_cloudsql(os.getenv("DB_USER"), os.getenv("DB_PASS"), os.getenv("DB_DATABASE"), os.getenv("DB_HOST"), "covid_data")
    print(temp.data_source)


# process_chunk: takes in a chunk of data and returns a list of data points
#   input: chunk of data, x column index, y column index
#   output: list of data points
def process_age_date_points(chunk, x_col, y_col):
    # Initialize an empty list to store (x, y) data points
    data_points = []

    # Iterate over each row in the chunk, accessing row data as named tuples
    for row in chunk.itertuples(index=False):
        # Append an (x, y) pair to the data_points list by extracting the values
        # at the specified x and y column indices, converting them to integers
        data_points.append((row[x_col], int(row[y_col])))

    # Return the list of data points for this chunk
    return data_points

def generate_age_date_plot(pixie_source, plot_func, x_index, y_index, plot_kwargs, label_kwargs, data_type='local', output_path='plot.png', verbose=False):
    # Start spinner animation
    if verbose:
        SPINNER.start()

    # Prepare arguments for each chunk of data by pairing each chunk with x and y indices
    chunk_args = [(chunk, x_index, y_index) for chunk in pixie_source]

    # Decide on the type of processing is needed, pooling is usually better for local file data,
    # threading is usually better for server based data
    if data_type == 'local':
        # Use a multiprocessing pool to process chunks
        processed_chunks = pool_task(process_age_date_points, chunk_args)
    else:
        # Use a threading to process chunks
        processed_chunks = thread_task(process_age_date_points, chunk_args)

    if verbose:
        print("Flattening data...")

    # Flatten the list of data points
    all_data_points = [point for sublist in processed_chunks for point in sublist]

    # Settings for scatterplot axis
    figure, axis = plt.subplots()
    axis.xaxis.set_major_locator(mdates.MonthLocator())
    axis.xaxis.set_major_formatter(mdates.DateFormatter('%b'))
    axis.yaxis.set_major_locator(mticker.MultipleLocator(5))

    if verbose:
        SPINNER.start("Plotting points using matplotlib...")

    start_main = time.time()
    for (x, y) in all_data_points:
        plot_func(x, y, **plot_kwargs)

    # Show the plot
    if verbose:
        SPINNER.start("Saving plot...")

    # Set title and axis labels
    plt.title('2020')
    plt.xlabel('Date of Death')
    plt.ylabel('Age')
    plt.ylim(0, None)
    plt.xlim(pd.to_datetime('1/1/2020'), pd.to_datetime('31/12/2020'))
    plt.xticks(fontsize='small')
    plt.savefig(output_path, format="png", dpi=300)

    plt.close()
    SPINNER.stop()

    if verbose:
        print(f"Processing time: #{(time.time() - start_main) / 60} minutes")
        return (time.time() - start_main) / 60


def test_age_date_plot():
    # CSV file contains a little over 2000 rows where date column is not set to a missing or empty value
    start = time.time()
    #Pixie.from_csv("../covid-data-date-filtered-small.csv", parse_dates=['DATE_DIED'], date_format='%d/%m/%Y')
    Pixie.from_csv("../covid-data.csv")
    return (time.time() - start) / 60
    #return generate_age_date_plot(temp.data_source, plt.plot, 4, 7, plot_kwargs, label_kwargs, "local", 'age-date-plot.png', True)

def calc_speedup(p, s):
    return ((s - p) / s)*100

if __name__ == '__main__':
    csv_time = {}
    postgresql_time = {}
    json_time = {}
    parquet_time = {}
    sqlite_time = {}

    for x in range(1):
        csv_time[x] = test_csv()
        postgresql_time[x] = test_postgresql()
        json_time[x] = test_json()
        parquet_time[x] = test_parquet()
        sqlite_time[x] = test_sqlite()
        print(f"Test {x} complete")

    print("Avg time for CSV:", sum(csv_time.values()) / len(csv_time))
    print("Avg time for PostgreSQL:", sum(postgresql_time.values()) / len(postgresql_time))
    print("Avg time for JSON:", sum(json_time.values()) / len(json_time))
    print("Avg time for Parquet:", sum(parquet_time.values()) / len(parquet_time))
    print("Avg time for SQLite:", sum(sqlite_time.values()) / len(sqlite_time))