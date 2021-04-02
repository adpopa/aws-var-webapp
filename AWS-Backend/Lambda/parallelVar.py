import queue
import threading

import boto3
import json

import math
import numpy as np

queue = queue.Queue()

def invoke_lambda(queue_values):
    invoke_lam = boto3.client("lambda", region_name="us-east-1")
    payload = {'queue_values': queue_values}
    resp = invoke_lam.invoke(FunctionName = "valueAtRisk", InvocationType = "RequestResponse", Payload = json.dumps(payload))
    response = json.loads(resp['Payload'].read().decode("utf-8"))
    return response['var_list']

def read_s3(file):
    s3 = boto3.client("s3")
    moving_average = []
    bucketname = 'emr-stocks'
    filename = 'output/{}/part-00000'.format(file)
    fileObj = s3.get_object(Bucket=bucketname, Key=filename)
    lines = fileObj['Body'].iter_lines()

    for row in lines:
        moving_average.append(row.decode("utf-8").split(','))

    return moving_average

class ThreadUrl(threading.Thread):
    def __init__(self, queue, task_id):
        threading.Thread.__init__(self)
        self.queue = queue
        self.task_id = task_id
        self.data = None

    def run(self):
        try:
            queue_values = self.queue.get()

            # CALL LAMBDA FUNCTION valueAtRisk
            var_list = invoke_lambda(queue_values)
            self.data = var_list
            self.queue.task_done()
        except Exception as e: print(e)

def parallel_run(params):
    ma_file = params[0]
    R =  params[1]
    S =  params[2]
    V =  params[3]
    queue_S = params[4] # S/R
    units = params[5]

    # read from s3 bucket
    moving_average = read_s3(ma_file)

    threads=[]
    for i in range(0, R):
        t = ThreadUrl(queue, i)
        threads.append(t)
        t.setDaemon(True)
        t.start()

    queue_values = [queue_S, V, moving_average]
    for x in range(0, R):
        queue.put(queue_values)

    queue.join()

    results = [t.data for t in threads]
    var_list = results[0]

    for results_index in range(0, len(var_list)):
        var_ninety_five = np.zeros(R)
        var_ninety_nine = np.zeros(R)
        for thread_number in range(0, len(results)):
            var_ninety_five[thread_number] = results[thread_number][results_index][1]
            var_ninety_nine[thread_number] = results[thread_number][results_index][2]
        var_ninety_five_average = np.mean(var_ninety_five[:].astype(float))
        var_ninety_nine_average = np.mean(var_ninety_nine[:].astype(float))
        var_list[results_index][1] = var_ninety_five_average
        var_list[results_index][2] = var_ninety_nine_average

    # index | profit/loss | units | price | price_obtained
    profit_loss = []

    index = int(var_list[0][0])
    line = moving_average[index]
    if i == 0 and line[3] == 'SELL':
        profit = float(line[1]) * units
        profit_loss.append([index, profit, 0, float(line[1]), line[3], profit])
    else:
        profit = 0 - units*float(line[1])
        profit_loss.append([index, profit, units, float(line[1]), line[3], 0])

    for i in range(1, len(var_list)):
        index = int(var_list[i][0])
        line = moving_average[index]

        if line[3] == 'SELL':
            units = profit_loss[i-1][2]
            if units == 0:
                obtained = profit_loss[i-1][5]
            profit = (units * float(line[1])) - (units * profit_loss[i-1][3])
            profit_loss.append([index, profit, profit_loss[i-1][2], float(line[1]), line[3], (units * float(line[1]))])
            var_list[i][1] = var_list[i][1] * units
            var_list[i][2] = var_list[i][2] * units
        else:
            obtained = 0
            units = math.floor(profit_loss[i-1][5]/float(line[1]))
            if units == 0:
                obtained = profit_loss[i-1][5]
            profit = (profit_loss[i-1][5])-(units*float(line[1]))
            profit_loss.append([index, profit, units, float(line[1]), line[3], obtained])
            var_list[i][1] = var_list[i][1] * units
            var_list[i][2] = var_list[i][2] * units

    return (moving_average,var_list,profit_loss)

def lambda_handler(event, context):
    ma_file = event['ma_file']
    R = int(event['R'])
    S = int(event['S'])
    V = int(event['V'])
    units = int(event['stockUnits'])
    queue_S = math.ceil(S/R)
    params = [ma_file, R, S, V, queue_S, units]
    moving_average, var_list, profit_loss = parallel_run(params)
    payload = {'moving_average': moving_average, 'var_list': var_list, 'profit_loss': profit_loss}

    return payload
