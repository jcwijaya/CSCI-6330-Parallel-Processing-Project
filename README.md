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
    
    # Syntax: Pixie.from_<type>(filename, chunks)>
    temp = Pixie.from_json('../covid-data.json', 82000)
    # Syntax: generate_chunked_plot_local(pixie_source, x_index, y_index, plot_kwargs, label_kwargs)
    generate_chunked_plot_local(temp.data_source, 7, 8, plot_kwargs, label_kwargs)
