import time

import concurrent
import concurrent.futures
import matplotlib.pyplot as plt
import multiprocessing as mp
from halo import Halo



# Set Matplotlib backend
plt.switch_backend('TkAgg')



# Create a Halo spinner object to call in functions
SPINNER = Halo(spinner='dots')



# validate_data: checks if all given columns exist in given data
#   input: list of column names, pandas.DataFrame
#   output: None
def validate_data(columns, data):
    # Iterate over each column name
    for col in columns:
        # Check if the column name exists in the DataFrame
        if col not in data.columns:
            # Raise an AttributeError if the column name is not found
            raise AttributeError(col + ' is not a valid column name.')
            
            
            
def remove_rows(value, chunk, column):
    for row in chunk.index:
        if chunk.loc[row, column] == value:
            print('dropping value' + str(chunk.loc[row, column]) + ' at ' + str(row) + ' ' + str(column))
            chunk.drop(row, axis=0, inplace=True)

    return chunk

  
  
#for each chunk of data will check if specified column has undesired value and delete rows that have it and also speed it up with parallelism
def clean_rows(value, chunks, column, num_processes=None):

    validate_data([column], chunks[0])

    results = pool_task(remove_rows, [(value, chunk, column) for chunk in chunks], num_processes)
    return results
            
    
            
#Method for transforming data of a column, will make use of parallelism
#Can iterate over chunks to transform columns, maybe also parallelize it with pool
#input: column to apply transformation on, chunked data, custom function to do transformation, additional args for function
def transform_column_data(column, chunks, userFunction, args, num_processes=None):
    #function to apply a user's function onto a column of a dataframe/chunk
    def transform_column_in_chunk(column, chunk, userFunction, args):
        chunk[column] = chunk[column].apply(userFunction, args=tuple(args))
        return chunk

    validate_data([column], chunks[0])

    results = pool_task(transform_column_in_chunk, [(column, chunk, userFunction, args) for chunk in chunks], num_processes)
    return results



# Function to replace specified values in a dataset, optionally in specific columns or across all columns by default
def replace_data(target_val, default_val, chunks, cols=None):

    # Inner function to replace values within a single chunk of data
    def replace_data_in_chunk(target_val, default_val, chunk, columns=None):
        # If no columns are specified, apply the replacement to all columns in the chunk
        if columns is None:
            columns = chunk.columns
        # Apply the replace_values function across specified columns in the chunk
        chunk[columns] = chunk[columns].apply(replace_values, args=(target_val, default_val), axis=0)
        return chunk

    # Validate that the specified columns (if any) are present in the data chunks
    SPINNER.start("Validating data...")
    if cols is not None:
        validate_data(cols, chunks[0])

    SPINNER.start("Replacing values...")
    # Process each chunk in parallel to replace values, using pool_task for multiprocessing
    results = pool_task(replace_data_in_chunk, [(target_val, default_val, chunk, cols) for chunk in chunks])

    # Stop the spinner
    SPINNER.stop()

    print("Operation complete.")

    # Return the list of processed chunks with replaced values
    return results



# replace_values: takes in a series of values and replaces the target value with the default value
#   input: series of values, target value, default value
#   output: series of values
def replace_values(column_vals, target_val, default_val):
    # Iterate over each item in the column_vals Series (index-value pairs)
    for index, val in column_vals.items():
        # Check if the current value matches the target value
        if val == target_val:
            # Replace the target value at this index with the default value
            column_vals.loc[index] = default_val

    # Return the modified Series with replaced values
    return column_vals



# process_chunk: takes in a chunk of data and returns a list of data points
#   input: chunk of data, x column index, y column index
#   output: list of data points
def process_chunk(chunk, x_col, y_col):
    # Initialize an empty list to store (x, y) data points
    data_points = []

    # Iterate over each row in the chunk, accessing row data as named tuples
    for row in chunk.itertuples(index=False):
        # Append an (x, y) pair to the data_points list by extracting the values
        # at the specified x and y column indices, converting them to integers
        data_points.append((int(row[x_col]), int(row[y_col])))

    # Return the list of data points for this chunk
    return data_points



# pool_task: Uses multiprocessing-lib to process some function in parallel
#   input: function, list of arguments
#   output: list of results
def pool_task(task_function, args, num_processes=None):
    # If no number of processes is specified, set it to the number of CPU cores
    if num_processes is None:
        num_processes = mp.cpu_count()

    # Create a pool of worker processes
    with mp.Pool(processes=num_processes) as pool:
        # Map the task_function to each argument set in args and collect results
        results = pool.starmap(task_function, args)

        # Close the pool
        pool.close()

        # Wait for all worker processes to finish their tasks
        pool.join()

        # Return the list of results from all completed tasks
        return results



def thread_task(task_function, args, num_threads=None):
    # If no number of threads is specified, set it to the number of CPU cores
    if num_threads is None:
        num_threads = mp.cpu_count()

    # Initialize an empty list to store results from each task
    results = []

    # Create a ThreadPoolExecutor to manage a pool of worker threads
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit each task to the executor, storing a mapping of futures to arguments
        future_to_args = {executor.submit(task_function, *arg): arg for arg in args}

        # Process each completed future as it finishes
        for future in concurrent.futures.as_completed(future_to_args):
            try:
                # Append the result of the task to the results list
                results.append(future.result())
            except Exception as exc:
                # Print an error message if the task raises an exception
                print(f'Task generated an exception: {exc}')

    # Return the list of results from all completed tasks
    return results


# generate plot(): function that takes in a pixie source, plot function, x index, y index, plot kwargs, label kwargs, type of data, and output path
#
# inputs:
#     pixie_source: Parallel Pixie object
#     plot_func: Type of plot function, either plt.plot, plt.scatter, or plt.bar
#     x_index: Index of x column
#     y_index: Index of y column
#     plot_kwargs: Keyword arguments for plot function
#     label_kwargs: Keyword arguments for title and axis labels
#     data_type: Type of data, either 'local' or 'server', defaults to local
#     output_path: Path to save plot, defaults to local path as plot.png
#     output_path: Path to save plot
#     verbose: Boolean to processing information.

def generate_plot(pixie_source, plot_func, x_index, y_index, plot_kwargs, label_kwargs, data_type='local', output_path='plot.png', verbose=False):
    # Start spinner animation
    SPINNER.start()

    # Prepare arguments for each chunk of data by pairing each chunk with x and y indices
    chunk_args = [(chunk, x_index, y_index) for chunk in pixie_source]

    # Decide on the type of processing is needed, pooling is usually better for local file data,
    # threading is usually better for server based data
    if data_type == 'local':
        # Use a multiprocessing pool to process chunks
        processed_chunks = pool_task(process_chunk, chunk_args)
    else:
        # Use a threading to process chunks
        processed_chunks = thread_task(process_chunk, chunk_args)

    if verbose:
        print("Flattening data...")

    # Flatten the list of data points
    all_data_points = [point for sublist in processed_chunks for point in sublist]

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    if verbose:
        SPINNER.start("Plotting points using matplotlib...")

    start_main = time.time()
    for (x, y) in all_data_points:
        plot_func(x, y, **plot_kwargs)

    # Show the plot
    if verbose:
        SPINNER.start("Saving plot...")

    plt.savefig(output_path, format="png", dpi=300)
    plt.close()
    SPINNER.stop()

    if verbose:
        print(f"Processing time: #{(time.time() - start_main) / 60} minutes")
        return (time.time() - start_main) / 60