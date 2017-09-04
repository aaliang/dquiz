import matplotlib.pyplot as plt
import numpy as np
import csv
import sys

def get_header_and_data(csvfile):
  csvreader = csv.reader(csvfile, delimiter=',')
  stuff = list(csvreader)
  header = stuff[0]
  data = stuff[1:]
  return header, data

def plot_size_breakdown(path_to_csv):
  with open(path_to_csv, 'r') as csvfile:
    header, data = get_header_and_data(csvfile)

    ind = np.arange(len(header) - 1)
    width = 0.10

    fig, ax = plt.subplots()

    plots = [ ax.bar(ind + i*width, d[1:], width) for i, d in enumerate(data) ]

    ax.set_xticks(ind + width / 2)
    ax.set_xticklabels(header[1:])

    ax.legend([p[0] for p in plots], [d[0] for d in data])

    ax.set_ylabel('Num Practices')
    ax.set_title('Practice Sizes Breakdown')

    plt.show()

def plot_daily(path_to_csv):
  with open(path_to_csv, 'r') as csvfile:
    csvreader = csv.reader(csvfile, delimiter=',')
    header, data = get_header_and_data(csvfile)

    ind = np.arange(len(header) - 1)
    width = 0.10

    fig, ax = plt.subplots()

    for d in data:
      state = d[0]
      p = ax.plot(ind, d[1:], label=state)

    ax.legend()

    ax.set_ylabel('Num Physicians')
    ax.set_title('Number of Physicians 2017-03-01 to 2017-05-01')
    ax.set_xticks([0, len(d) - 2])
    ax.set_xticklabels([header[1], header[-1]])

    plt.show()

if  __name__ == '__main__':
  plot_type = sys.argv[1]
  path_to_csv = sys.argv[2]

  if plot_type == 'breakdown':
    plot_size_breakdown(path_to_csv)
  elif plot_type == 'daily':
    plot_daily(path_to_csv)
