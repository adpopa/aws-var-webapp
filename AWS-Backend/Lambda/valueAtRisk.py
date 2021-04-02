import numpy as np
import random
import json

def valueAtRisk(queue_values):
    S, V, moving_average = queue_values
    confidence = [0.95, 0.99]

    # date | price | signal | VAR 95% | VAR 99%
    var_list = []
    # FIND THE INDEX OF ALL SIGNALS STARTING WITH THE SIGNAL WITH AT LEAST V DATAPOINTS BEFORE
    signal_index_list = []
    for i in range(0, len(moving_average)):
        if moving_average[i][3].isupper() and i > V:
            signal_index_list.append(i)
    # CALCULATE THE VAR FOR EACH SIGNAL
    for i in range(0, len(signal_index_list)):
        signal_start_index = signal_index_list[i]
        return_profit = []
        mc_samples = []

        for j in range(signal_start_index-V+1, signal_start_index):
            today = float(moving_average[j][1])
            yesterday = float(moving_average[j-1][1])
            profit = (today-yesterday)/yesterday
            return_profit.append(profit)

        mu = np.mean(return_profit)
        sigma = np.std(return_profit)
        # GENERATE MC VALUES
        for x in range(0,S):
            mc_samples.append(random.gauss(mu, sigma))

        if moving_average[signal_start_index][3] == 'BUY':
            mc_samples.sort(reverse=True)
        else:
            mc_samples.sort()

        new_price = []
        for i in range(0,len(confidence)):
            mc_index = int(S * confidence[i])
            new_price.append((1+mc_samples[mc_index]) * float(moving_average[signal_start_index][1]))

        # signal_index | VAR 95% | VAR 99%
        var_list.append([int(signal_start_index), new_price[0], new_price[1]])

    return var_list

def lambda_handler(event, context):
    try:
        queue_values = event['queue_values']
        var_list = valueAtRisk(queue_values)
        response = {'var_list': var_list}
        return response
    except Exception as e: print(e)

    return 'Should not get here'
