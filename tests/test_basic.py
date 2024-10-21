# Function that starts a clock, runs a function, and returns the elapsed time. Used to measure parallel performance.
import array
import sqlite3
import time
from datetime import datetime
import pandas as pd
from matplotlib import pyplot as plt

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
    # Load the entire dataset
    df = pd.read_csv("../covid-data.csv")

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    for _, row in df.iterrows():
        x = int(row.iloc[7])
        y = int(row.iloc[8])
        plt.plot(x, y, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.show()


# Test JSON
def test_json():
    # Load the entire JSON dataset
    df = pd.read_json("../covid-data.json")

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    for _, row in df.iterrows():
        x = int(row.iloc[7])
        y = int(row.iloc[8])
        plt.plot(x, y, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.show()

# Test Parquet
def test_parquet():
    # Load the entire Parquet dataset
    df = pd.read_parquet("../covid-data.parquet")

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    for _, row in df.iterrows():
        x = int(row.iloc[7])
        y = int(row.iloc[8])
        plt.plot(x, y, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.show()

# Test SQLite
def test_sqlite():
    # Connect to the SQLite database
    conn = sqlite3.connect("../covid-data.db")

    # Load the dataset from an SQL query
    df = pd.read_sql_query("SELECT * FROM covid_data", conn)

    # Close the connection
    conn.close()

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    for _, row in df.iterrows():
        x = int(row.iloc[7])
        y = int(row.iloc[8])
        plt.plot(x, y, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.show()

if __name__ == '__main__':
    with open("../covid-data.csv", 'r') as f:
        row_count = sum(1 for _ in f) - 1  # Subtract 1 to exclude header
    print("Number of rows:", row_count)


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