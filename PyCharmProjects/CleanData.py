import numpy as np
import pandas as pd
import datetime as dt
import math
import os

# todo collapse time into night, morning rush hour, day time, afternoon rush hour.
# todo make links symmetric
# todo check code for duplicates

raw_data = pd.DataFrame()

# find all files
path = 'M:/GitHub/Anomaly-Detection/Small'

files = []
for r, d, f in os.walk(path):
    for file in f:
        if '.csv' in file:
            files.append(os.path.join(r, file))

for f in files:
    file = pd.read_csv(f)

    # determine type of each file
    if 'fhv' in f:
        file = (file.drop(columns=file.columns[4:]))
    elif 'yellow' in f:
        file = (file.drop(columns=file.columns[0])
                    .drop(columns=file.columns[3:7])
                    .drop(columns=file.columns[9:]))
    else:
        file = (file.drop(columns=file.columns[0])
                .drop(columns=file.columns[3:5])
                .drop(columns=file.columns[7:]))

    file.columns = ['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location']
    file = file.dropna()
    file['PU_Location'] = file['PU_Location'].astype(int)
    file['DO_Location'] = file['DO_Location'].astype(int)

    # add appropriate columns from each file to concantenated full dataframe
    raw_data = pd.concat([raw_data, file], ignore_index=True)

# apply function to full dataframe

# Comment this for the full data set
# another change
# raw_data = raw_data.iloc[:100, :]


def convert_str_to_datetime(data):
    return data.map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))


raw_data.iloc[:, 0] = convert_str_to_datetime(raw_data.iloc[:, 0])
raw_data.iloc[:, 1] = convert_str_to_datetime(raw_data.iloc[:, 1])

raw_data['JourneyTime'] = raw_data.iloc[:, 1] - raw_data.iloc[:, 0]
raw_data['StartHour'] = raw_data.iloc[:, 0].map(lambda x: x.hour)


def calculate_datetime_average(data):
    average_time_by_hour = pd.Series(index=range(24))
    for hour in range(24):
        if [journey_time.seconds for journey_time in data[data['StartHour'] == hour]['JourneyTime']]:
            average_time_by_hour[hour] = (np.mean([journey_time.seconds for journey_time
                                                  in data[data['StartHour'] == hour]['JourneyTime']]))
        else:
            average_time_by_hour[hour] = 0
    return average_time_by_hour


average_time = (raw_data.groupby(['PU_Location', 'DO_Location']).apply(calculate_datetime_average))
print(average_time.index)

# a change
