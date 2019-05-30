import numpy as np
import pandas as pd
import datetime as dt
import math
import os

# todo collapse time into night, morning rush hour, day time, afternoon rush hour.
# todo make links symmetric
# todo check code for duplicates
# todo add gitignore for extra files

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

time_categories = pd.DataFrame(data=[[dt.time.min, dt.time(8)],
                                     [dt.time(8), dt.time(10)],
                                     [dt.time(10), dt.time(16)],
                                     [dt.time(16), dt.time(18)],
                                     [dt.time(18), dt.time.max]],
                               index=['Morning', 'MorningRush', 'Day', 'AfternoonRush', 'Night'],
                               columns=['StartTime', 'EndTime'])


def convert_str_to_datetime(data):
    return data.map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))


def get_time_category(data):
    for category in time_categories.index:
        if time_categories.loc[category, 'StartTime'] < data.time <= time_categories.loc[category, 'EndTime']:
            return category


raw_data.iloc[:, 0] = convert_str_to_datetime(raw_data.iloc[:, 0])
raw_data.iloc[:, 1] = convert_str_to_datetime(raw_data.iloc[:, 1])

raw_data['JourneyTime'] = raw_data.loc[:, 'DO_Datetime'] - raw_data.loc[:, 'PU_Datetime']
raw_data['StartTime'] = raw_data.loc[:, 'PU_Datetime'].map(lambda x: x.time)
raw_data['Link'] = [{x, y} for x, y in zip(raw_data['PU_Location'], raw_data['DO_Location'])]
raw_data['TimeCategory'] = raw_data.loc[:, 'PU_Datetime'].apply(get_time_category)

# todo add time category to each entry
# or even a column for each time category (do this in non-hardcode way)
# then use timecat to compare in groupby function

print(raw_data)

#
# def which_time_category(time):


def time_in_category(time, category):
    return time_categories.loc[category, 'StartTime'] < time < time_categories.loc[category, 'EndTime']


def calculate_datetime_average(data):
    # average_time_by_hour = pd.Series(index=range(24))
    # for hour in range(24):
    #     if [journey_time.seconds for journey_time in data[data['StartHour'] == hour]['JourneyTime']]:
    #         average_time_by_hour[hour] = (np.mean([journey_time.seconds for journey_time
    #                                               in data[data['StartHour'] == hour]['JourneyTime']]))
    #     else:
    #         average_time_by_hour[hour] = 0
    # return average_time_by_hour

    average_time_by_category = pd.Series(index=time_categories.index)
    for category in time_categories.index:
        if [journey_time.seconds for journey_time in data[data['TimeCategory'] == category]['JourneyTime']]:
            average_time_by_category[category] = (np.mean([journey_time.seconds for journey_time
                                                  in data[data['TimeCategory'] == category]['JourneyTime']]))
        else:
            average_time_by_category[category] = 0


average_time = (raw_data.groupby('Link').apply(calculate_datetime_average))
print(average_time)

# a change
