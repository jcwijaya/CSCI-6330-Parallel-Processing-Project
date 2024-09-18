import matplotlib.pyplot as plt
import multiprocessing as mp

# Run checks on the environment to see what needs to be changed for the program to run correctly.
# For example, if the user is running Windows there are different multiprocessing options that need
# to be considered versus Linux or macOS.
def check_environment():
    pass

# Take in the data submitted to the class and make sure it is valid for whatever the user is attempting
# to do to it. For example, if the user is trying to make a pie chart using all columns then we need to
# remove the headers from the data. Or alternatively, if the user is trying to make a plot and requests columns
# that don't exist we need to throw an error because pandas will try to parse the data anyway.
def validate_data():
    pass

# This function will process each chunk of data return a list of data points that will be plotted.
def process_chunk(chunk, x_col, y_col):
    data_points = []
    for row in chunk.itertuples(index=False):
        # Append [x,y] pairs to the data_points list
        data_points.append((row[x_col], row[y_col]))
    return data_points

# I don't know if we'll need this function yet. This would theoretically be used
# to flatten the list of points that were returned from the process_chunk function.
# As long as we can just use chunks we won't need it but if we need to find another way
# for Parquet and JSON files then we'll need to implement this.
def aggregate_results(results):
    pass

# Set the parameters for the plot. For example, the title, the axis labels, x/y limits, color scheme, etc.
def generate_chunked_plot(pixie_source, x_index, y_index, plot_kwargs, label_kwargs):

    # pixie_source is a list of dataframes from the Pixie class, each dataframe in the list is a chunk of data
    # This simply separates the dataframes into a list of "chunks"
    chunks = [chunk for chunk in pixie_source]

    # Create a Pool of worker processes
    with mp.Pool() as pool:
        # Map chunks to the process_chunk function.
        # results is a list of lists of data points and looks like [[(x1, y1), (x2, y2)], [(x3, y3), (x4, y4)]]
        # where each list in results is a chunk of data points.
        results = pool.starmap(process_chunk, [(chunk, x_index, y_index) for chunk in chunks])

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
