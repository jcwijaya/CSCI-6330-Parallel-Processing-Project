# Function that starts a clock, runs a function, and returns the elapsed time. Used to measure parallel performance.
import time
import psycopg2
import pandas as pd
import sqlite3
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
    start1 = time.time()
    # Load the entire dataset
    df = pd.read_csv("../linear_xy_large.csv")

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    x_pts, y_pts = [int(row[0]) for row in df.itertuples(index=False)], [int(row[1]) for row in df.itertuples(index=False)]
    plt.plot(x_pts, y_pts, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.savefig("plot.png", format="png", dpi=300)
    return time.time() - start1



# Test JSON
def test_json():
    start2 = time.time()
    # Load the entire JSON dataset
    df = pd.read_json("../linear_xy_large.json")

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    x_pts, y_pts = [int(row[0]) for row in df.itertuples(index=False)], [int(row[1]) for row in df.itertuples(index=False)]
    plt.plot(x_pts, y_pts, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.savefig("plot.png", format="png", dpi=300)
    return time.time() - start2



# Test Parquet
def test_parquet():
    start3 = time.time()
    # Load the entire Parquet dataset
    df = pd.read_parquet("../linear_xy_large.parquet")

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    x_pts, y_pts = [int(row[0]) for row in df.itertuples(index=False)], [int(row[1]) for row in df.itertuples(index=False)]
    plt.plot(x_pts, y_pts, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.savefig("plot.png", format="png", dpi=300)
    return time.time() - start3



# Test SQLite
def test_sqlite():
    start4 = time.time()
    # Connect to the SQLite database
    conn = sqlite3.connect("../linear_xy_large.db")

    # Load the dataset from an SQL query
    df = pd.read_sql_query("SELECT * FROM xy_data", conn)

    # Close the connection
    conn.close()

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    x_pts, y_pts = [int(row[0]) for row in df.itertuples(index=False)], [int(row[1]) for row in df.itertuples(index=False)]
    plt.plot(x_pts, y_pts, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.savefig("plot.png", format="png", dpi=300)

    return time.time() - start4



# Test PostgreSQL
def test_postgresql():
    start5 = time.time()

    # Connect to the PostgreSQL database
    conn = psycopg2.connect(
        dbname="linear_xy_large",
        user="postgres",
        password="postgres",
        host="localhost",
        port="5432"
    )

    # Load the dataset from an SQL query
    df = pd.read_sql_query("SELECT * FROM xy_data", conn)

    # Close the connection
    conn.close()

    # Initialize the plot
    plt.figure()

    # Iterate over rows and plot each point
    x_pts, y_pts = [int(row[0]) for row in df.itertuples(index=False)], [int(row[1]) for row in df.itertuples(index=False)]
    plt.plot(x_pts, y_pts, **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.savefig("plot.png", format="png", dpi=300)

    return time.time() - start5



if __name__ == '__main__':
    csv_time = {}
    json_time = {}
    parquet_time = {}
    sqlite_time = {}
    postgresqlite_time = {}

    for x in range(1):
        csv_time[x] = test_csv()
        json_time[x] = test_json()
        parquet_time[x] = test_parquet()
        sqlite_time[x] = test_sqlite()
        postgresqlite_time[x] = test_postgresql()
        print(f"Test {x} complete")

    print("Avg time for CSV:", sum(csv_time.values()) / len(csv_time))
    print("Avg time for PostgreSQL:", sum(postgresqlite_time.values()) / len(postgresqlite_time))
    print("Avg time for JSON:", sum(json_time.values()) / len(json_time))
    print("Avg time for Parquet:", sum(parquet_time.values()) / len(parquet_time))
    print("Avg time for SQLite:", sum(sqlite_time.values()) / len(sqlite_time))

    print(csv_time)
    print(postgresqlite_time)
    print(json_time)
    print(parquet_time)
    print(sqlite_time)