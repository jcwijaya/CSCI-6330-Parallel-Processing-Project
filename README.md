# CSCI 6330: Parallel Processing Project

### Team Members:

- Jessica Wijaya
- Freyja Richardson
- Junhui Shen

### Overview:

For this research project, we propose a Python implementation of parallel processing for data analysis. Specifically,
we've designed a tool to increase computation speeds when processing large CSV, Parquet, SQLite, and JSON files as well
as data pulled from online databases like PostgreSQL and Google Cloud for the purpose of data analysis and visualization
via Pandas and MatPlotLib respectively.

### Installation:

1. Download and unzip the master code zip.
2. Run `pip install -r requirements.txt` to install the dependencies.
3. Run `pip install <version>.zip` to install the project.

### Usage:

Simply create an instance of the Pixie class and pass it the data source you want to process.

##### Example:

```python
from pixie import Pixie
    
def test_json():
    # Create plot kwargs
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
    
    # Syntax: Pixie.from_<type>(filename, chunks (if unspecified, optimal chunks are determined automatically))>
    temp = Pixie.from_json('../covid-data.json')
    
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
    generate_plot(temp.data_source, plt.plot, 0, 1, plot_kwargs, label_kwargs, "local", 'linear-data.png', True)
```