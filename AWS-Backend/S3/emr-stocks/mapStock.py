#!/usr/bin/python3
import sys

lines = 0

data_points = []

for line in sys.stdin:
    # ignore the header info
    lines += 1
    if(lines < 2):
        continue
    #Date,Open,High,Low,Close,Adj Close,Volume
    data = line.split(",") # split via ,

    timestamp = data[0].split("/")
    date = timestamp[2]+"/"+timestamp[1]+"/"+timestamp[0]
    date = date.strip()

    adj_close = data[4]

    data_points.append([date, adj_close])

for row in data_points:
    print(row[0],'\t',row[1])
