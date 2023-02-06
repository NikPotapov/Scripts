from datetime import datetime as dt
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from warnings import simplefilter
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)

# get file from outlook in the future
file = r'C:\Users\nikita.potapov\OneDrive - NTT\Desktop\LogicMonitor\PUNDC Top 100 Interfaces Mbps - 1000 ' \
       r'UTC-20221121100036.html'
date_string = file[-19:-5]
max_data_list = []
legend_data_list = []

# read extracted file and parse script tab
with open(file, 'r') as f:
    doc = BeautifulSoup(f, 'html.parser')

script = str(doc.find('script'))
match = re.search('"timestamps"', script)


def find_index(term, ch):
    """Find all needful indexes with ch in str
    """
    for i in range(len(term)):
        if term[i:i + len(ch)] == ch:
            yield i


# indexes of the first timestamp list and create df
startTimestamps = list(find_index(script, '"timestamps":['))[0] + 14
endTimestamps = list(find_index(script, '"type":"graph"'))[0] - 2
timestamps_data = script[startTimestamps:endTimestamps].split(',')
df = pd.DataFrame()
df['Timestamps'] = np.array(timestamps_data)
df['Timestamps'] = pd.to_datetime(df['Timestamps'], unit='ms')
df['Timestamps'] = df['Timestamps'].dt.strftime('%d-%m-%Y %I:%M:%S')

# get indexes of data, max, legend
index_data = list(map(lambda x: x+8, list(find_index(script, '"data":['))))
index_max = list(map(lambda x: x+8, list(find_index(script, '],"max":'))))
index_legend = list(map(lambda x: x+10, list(find_index(script, '"legend":"NW'))))

for num in range(0, len(index_data)):
    max_value = script[index_max[num]:index_max[num]+10][:script[index_max[num]:index_max[num]+10].find(',')]
    legend_value = script[index_legend[num]:index_legend[num]+55][:script[index_legend[num]:index_legend[num]+55].find('"')]
    df[legend_value] = list(script[index_data[num]:index_max[num]-8].split(','))
    df[legend_value] = df[legend_value].astype('float')


df.to_excel('LogicMonitor ' + date_string + '.xlsx',
            index=False,
            sheet_name='LogicMonitor_export')














