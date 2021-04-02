import os
import logging
import json
import numpy as np

import codecs

from flask import Flask, request, render_template, jsonify

app = Flask(__name__)

def doRender(tname, values={}):
    if not os.path.isfile( os.path.join(os.getcwd(), 'templates/'+tname) ):
        return render_template('index.htm')
    return render_template(tname, **values)

def datetime(arr):
    for i in range(0, len(arr)):
        arr[i] = arr[i].replace('/','-')
    return np.array(arr, dtype=np.datetime64)

def chart(json_data, stock):
    from bokeh.embed import components
    from bokeh.plotting import figure
    from bokeh.resources import INLINE

    moving_average = np.asarray(json_data['moving_average'])
    var_list = np.asarray(json_data['var_list'])
    profit_loss = np.asarray(json_data['profit_loss'])

    total_profit_loss = 0
    for line in profit_loss:
        if line[4] == 'SELL':
            total_profit_loss += float(line[1])
    # return total_profit_loss

    var_95 = np.mean(var_list[:,1])
    var_99 = np.mean(var_list[:,2])

    table_arr = []

    for i in range(0, len(profit_loss)):
        table_arr.append([ moving_average[int(var_list[i][0])][0],profit_loss[i][4], profit_loss[i][1], profit_loss[i][3], var_list[i][1], var_list[i][2] ])

    signal_list_sell = np.empty((0,3), str)
    signal_list_buy = np.empty((0,3), str)

    for line in var_list:
        i = int(line[0])
        if(moving_average[i][3] == 'BUY'):
            signal_list_buy = np.append(signal_list_buy, np.array([[moving_average[i][0], moving_average[i][1], moving_average[i][2]]]), axis=0)
        else:
            signal_list_sell = np.append(signal_list_sell, np.array([[moving_average[i][0], moving_average[i][1], moving_average[i][2]]]), axis=0)

    p1 = figure(x_axis_type="datetime", title="Stock Closing Prices with Average Value", plot_height=500)
    p1.sizing_mode = 'stretch_width'
    p1.grid.grid_line_alpha=0.1
    p1.xaxis.axis_label = 'Date'
    p1.yaxis.axis_label = 'Price'

    # adj_close
    p1.line(datetime(moving_average[:,0]), moving_average[:,1].astype('float'), color='lightblue', legend_label=stock)
    # moving_average
    p1.line(datetime(moving_average[:,0]), moving_average[:,2].astype('float'), color='plum', legend_label='Moving Average')
    # signals buy
    p1.segment(datetime(signal_list_buy[:,0]), signal_list_buy[:,1].astype('float'), datetime(signal_list_buy[:,0]), signal_list_buy[:,2].astype('float'), color='green', legend_label='Signal Buy')
    # signals sell
    p1.segment(datetime(signal_list_sell[:,0]), signal_list_sell[:,1].astype('float'), datetime(signal_list_sell[:,0]), signal_list_sell[:,2].astype('float'), color='red', legend_label='Signal Sell')

    p1.legend.location = "top_left"

    # grab the static resources
    js_resources = INLINE.render_js()
    css_resources = INLINE.render_css()
    script, div = components(p1)
    return doRender( 'index.htm', { 'plot_div': div,
                                    'plot_script': script,
                                    'js_resources': js_resources,
                                    'css_resources': css_resources,
                                    'table_arr': table_arr,
                                    'total_profit_loss': total_profit_loss,
                                    'var_95': var_95,
                                    'var_99': var_99
                                    }
    )

@app.route('/hello')

def hello():
    return 'Hello World!'

@app.route('/map', methods=['POST'])
def mapHandler():
    import http.client
    import json
    if request.method == 'POST':
        stockSymbol = request.form.get('stockSymbol')
        movingAverage = request.form.get('movingAverage')

        if stockSymbol == '' or movingAverage == '':
            return doRender('index.htm',
            {'note': 'Please fill in the form !'})
        else:
            c = http.client.HTTPSConnection('1ctz05iqn7.execute-api.us-east-1.amazonaws.com')
            foo = {
                    'A': movingAverage,
                    'stockName': stockSymbol
                }
            json_data = json.dumps(foo)
            print(json_data)

            c.request("POST", "/default/emr_lambda", json_data)

            response = c.getresponse()
            data = json.loads(response.read())

            return doRender('index.htm', {'note': data})
    return 'Should not ever get here'

# POST method to get the VAR and do the chart
@app.route('/calculate', methods=['POST'])
def calculateHandler():
    import http.client
    import json
    if request.method == 'POST':
        stockUnits = request.form.get('stockUnits')
        stockSymbol = request.form.get('stockSymbol')
        movingAverage = request.form.get('movingAverage')
        varPeriod = request.form.get('varPeriod')
        mcSamples = request.form.get('mcSamples')
        numberResources = request.form.get('numberResources')

        if stockSymbol == '' or movingAverage == '' or varPeriod == '' or mcSamples == '' or numberResources == '':
            return doRender('index.htm',
            {'note': 'Please fill in the form !'})
        else:
            c = http.client.HTTPSConnection('ae5gr9ge28.execute-api.us-east-1.amazonaws.com')
            foo = {
                    'ma_file': 'A{}{}'.format(movingAverage,stockSymbol),
                    'R': numberResources,
                    'S': mcSamples,
                    'V': varPeriod,
                    'stockUnits': stockUnits
                }

            json_data = json.dumps(foo)

            print(json_data)

            c.request("POST", "/default/parallelVar", json_data)
            response = c.getresponse()
            data = json.loads(response.read())

            return chart(data, stockSymbol)
        return 'Should not ever get here'

# catch all other page requests - doRender checks if a page is available (shows it) or not (index)
@app.route('/', defaults={'path': ''})
@app.route('/<path:path>')
def mainPage(path):
    return doRender(path)

@app.errorhandler(500)
# A small bit of error handling
def server_error(e):
    logging.exception('ERROR!')
    return """
    An error occurred: <pre>{}</pre>
    """.format(e), 500

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
