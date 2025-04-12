import parallelpixie.processors as pp
import parallelpixie.pixie as Pixie
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.dates as mdates
import matplotlib.ticker as mticker

plot_kwargs = {
    'color': 'pink',
    'marker': '.',
    'linewidth': 2
}

label_kwargs = {
    'title': '2020 Covid Death Data',
    'xlabel': 'Date of Death',
    'ylabel': 'Age',
    'x_locator': mdates.MonthLocator(),
    'x_formatter': mdates.DateFormatter('%b'),
    'y_locator': mticker.MaxNLocator (21),
    'x-lim': (pd.to_datetime('1/1/2020', dayfirst=True), pd.to_datetime('31/12/2020', dayfirst=True)),
    'y-lim': (0, 105),
    'x-ticks': "fontsize=\'small\'"
}

def main():
    pp.generate_plot(Pixie.Pixie.from_csv("../covid-data-date-filtered-small.csv").data_source,
                            plt.scatter,
                            4,
                            7,
                            plot_kwargs,
                            label_kwargs,
                            "local",
                            'filtered-data.png',
                            True)

if __name__ == "__main__":
    main()