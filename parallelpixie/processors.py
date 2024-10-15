import matplotlib.pyplot as plt
import multiprocessing as mp

# Take in the data submitted to the class and make sure it is valid for whatever the user is attempting
# to do to it. For example, if the user is trying to make a pie chart using all columns then we need to
# remove the headers from the data. Or alternatively, if the user is trying to make a plot and requests columns
# that don't exist we need to throw an error because pandas will try to parse the data anyway.
#input: list of column names, chunked data
#output: throws exception if columns do not match dataframe headers
def validate_data(columns, data):
    print(data.columns)
    for col in columns:
        if col not in data.columns:
            raise AttributeError(col + ' is not a valid column name.')

# TODO: for each chunk of data will check if specified cols have undesired value and delete rows that have it and also speed it up with parallelism
def clean_rows(value, data, columns=None):
    pass

#Method for transforming data of a column, will make use of parallelism
#Can iterate over chunks to transform columns, maybe also parallelize it with pool
#input: column to apply transformation on, chunked data, custom function to do transformation, additional args for function
def transform_column_data(column, chunks, userFunction, args, num_processes=None):

    validate_data([column], chunks[0])
    print("Data is validated")

    results = pool_task(transform_column_in_chunk, [(column, chunk, userFunction, args) for chunk in chunks], num_processes)
    return results

#function to apply a user's function onto a column of a dataframe/chunk
def transform_column_in_chunk(column, chunk, userFunction, args):
    chunk[column] = chunk[column].apply(userFunction, args=tuple(args))
    return chunk

#function to replace values in dataset, can specify which columns to do replacement or by default will try to do for all
def replace_data(target_val, default_val, chunks, columns=None, num_processes=None):
    #Use one of the chunks to check if specified columns are valid
    if columns is not None:
        validate_data(columns, chunks[0])
    print("Data is validated")

    results = pool_task(replace_data_in_chunk, [(target_val, default_val, chunk, columns) for chunk in chunks], num_processes)
    return results

def replace_data_in_chunk(target_val, default_val, chunk, columns=None):
    if columns is None:
        columns = chunk.columns
    chunk[columns] = chunk[columns].apply(replace_values, args=(target_val, default_val), axis=0)

    return chunk

#current_val: value being processed
#target_val: value to remove from dataset
#default_val: new value to return if current_val equals to target_val
def replace_values(column_vals, target_val, default_val):
    for index, val in column_vals.items():
        if val == target_val:
            column_vals.loc[index] = default_val

    return column_vals

# This function will process each chunk of data return a list of data points that will be plotted.
def process_chunk(chunk, x_col, y_col):
    data_points = []
    for row in chunk.itertuples(index=False):
        # Append [x,y] pairs to the data_points list
        data_points.append((row[x_col], row[y_col]))
    return data_points

#General function for passing in task to be distributed into pool and return a result
# input: function to be executed,args for function, number of processes?
# returns results;
def pool_task(task_function, args, num_processes=None):
    if num_processes is None:
        num_processes = mp.cpu_count()

    with mp.Pool(processes=num_processes) as pool:
        results = pool.starmap(task_function, args,)

    pool.close()
    pool.join()

    return results

# Set the parameters for the plot. For example, the title, the axis labels, x/y limits, color scheme, etc.
def generate_chunked_plot(pixie_source, x_index, y_index, plot_kwargs, label_kwargs):

    # Map chunks to the process_chunk function.
    # results is a list of lists of data points and looks like [[(x1, y1), (x2, y2)], [(x3, y3), (x4, y4)]]
    # where each list in results is a chunk of data points.
    results = pool_task(process_chunk, [(chunk, x_index, y_index) for chunk in pixie_source])

    # Flatten the results and plot, this changes it so instead of the chunks being separate lists they are combined into
    # a list of [x,y] pairs
    all_data_points = [point for sublist in results for point in sublist]

    # Plot the data
    for pair in all_data_points:
        plt.plot(pair[0], pair[1], **plot_kwargs)

    # Set title and axis labels
    plt.title(label_kwargs['title'])
    plt.xlabel(label_kwargs['xlabel'])
    plt.ylabel(label_kwargs['ylabel'])

    # Show the plot
    plt.show()
