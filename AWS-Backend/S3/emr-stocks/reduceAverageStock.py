#!/usr/bin/python3

import sys

A = int(sys.argv[1]) # parameter

data_points = []
moving_average = []

days = 0;
date = None

sum = 0
for line in sys.stdin:
    current_date, current_close = line.split()
    data_points.append([current_date, current_close])

data_points = data_points[::-1]

# calculate moving average with for the specified time window A
for i in range(0,len(data_points)-A+1):
    current_date = data_points[i][0]
    current_close = float(data_points[i][1])
    for j in range(i,i+A):
        sum += float(data_points[j][1])

    average = sum/A
    sum = 0


    if (average - current_close) > 0:
        signal_type = "buy"
    else:
        signal_type = "sell"

    moving_average.append([current_date, str(current_close), str(average), signal_type])

moving_average = moving_average[::-1]

for i in range (1, len(moving_average)):
    if moving_average[i-1][3].lower() != moving_average[i][3].lower():
            moving_average[i][3] = moving_average[i][3].upper()

for row in moving_average:
    print (','.join(row))
