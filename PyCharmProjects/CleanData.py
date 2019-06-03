# import numpy as np
import pandas as pd
import datetime as dt
# import math
import os

# todo implement congestion factor method
# todo implement percentile factor method


time_categories = pd.DataFrame(data=[[dt.time.min, dt.time(8)],
                                     [dt.time(8), dt.time(10)],
                                     [dt.time(10), dt.time(16)],
                                     [dt.time(16), dt.time(18)],
                                     [dt.time(18), dt.time.max]],
                               index=['Morning', 'MorningRush', 'Day', 'AfternoonRush', 'Night'],
                               columns=['StartTime', 'EndTime'])


def find_individual_raw_data():
    path = 'M:/GitHub/Anomaly-Detection/Individual/yellow_tripdata_2018-01.csv'
    return pd.read_csv(path)


def find_multiple_raw_data():
    print('Getting multiple raw data files...')
    path = 'M:/GitHub/Anomaly-Detection/Multiple'

    files = []
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))

    data = pd.DataFrame()

    print('Cleaning data...')
    for file in files:

        file_data = pd.read_csv(file)

        if 'fhv' in file:
            data = pd.concat([data, clean_raw_data(file_data, 'fhv')], ignore_index=True)
        elif 'yellow' in file:
            data = pd.concat([data, clean_raw_data(file_data, 'yellow')], ignore_index=True)
        elif 'green' in file:
            data = pd.concat([data, clean_raw_data(file_data, 'green')], ignore_index=True)
        else:
            print('raw data error')

    print('Data cleaned.')
    return data


def clean_raw_data(data, data_format):
    print('Cleaning...')
    if data_format == 'fhv':
        data = (data.drop(columns=data.columns[4:]))
    elif data_format == 'yellow':
        data = (data.drop(columns=data.columns[0])
                    .drop(columns=data.columns[3:7])
                    .drop(columns=data.columns[9:]))
    elif data_format == 'green':
        data = (data.drop(columns=data.columns[0])
                .drop(columns=data.columns[3:5])
                .drop(columns=data.columns[7:]))
    else:
        print('type error')

    data.columns = ['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location']
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data['PU_Location'] = data['PU_Location'].astype(int)
    data['DO_Location'] = data['DO_Location'].astype(int)

    def convert_str_to_datetime(current_data):
        return current_data.map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'))

    data.iloc[:, 0] = convert_str_to_datetime(data.iloc[:, 0])
    data.iloc[:, 1] = convert_str_to_datetime(data.iloc[:, 1])

    data['JourneyTime'] = [journey.seconds for journey in (data.loc[:, 'DO_Datetime'] - data.loc[:, 'PU_Datetime'])]
    data['Link'] = [tuple({x, y}) for x, y in zip(data['PU_Location'], data['DO_Location'])]

    def get_time_category(current_data):
        for category in time_categories.index:
            if time_categories.loc[category, 'StartTime'] < current_data.time() \
                    <= time_categories.loc[category, 'EndTime']:
                return category

    data['TimeCategory'] = data.loc[:, 'PU_Datetime'].apply(get_time_category)
    data = (data.drop(columns=data.columns[0:4]))

    return data


def calculate_average_journey_time(data):
    print('Calculating average journey time...')
    average_time = data.groupby(['Link', 'TimeCategory']).mean().unstack('TimeCategory')
    average_time.columns = time_categories.index
    print('Average journey time calculated.')
    return average_time


def run_congestion_factor_method(c_data, h_data, congestion_factor):
    print('Running congestion method...')
    for index in range(c_data.shape[0]):
        c_jt = c_data.iloc[index, 0]
        c_link = c_data.iloc[index, 1]
        c_cat = c_data.iloc[index, 2]

        print('c jt', c_jt, 'c_link', c_link, 'c_cat', c_cat, end='\n')
        print('h_jt', h_data.loc[c_link, c_cat])

    # c_data['IsCongested'] = [c_data.iloc[index, 0] >=
    #                          congestion_factor * h_data.loc[c_data.iloc[index, 1], c_data.iloc[index, 2]]
    #                          for index in range(c_data.shape[0])]
    print('Congestion method complete.')
    return c_data


# raw_data = find_individual_raw_data()
# print(raw_data.head())
# clean_data = clean_raw_data(raw_data.iloc[:100, :], 'yellow')

clean_data = find_multiple_raw_data()
# print(clean_data)
historical_data = calculate_average_journey_time(clean_data)
# print(historical_data)
congested_data = run_congestion_factor_method(clean_data, historical_data, congestion_factor=1.8)
print(congested_data.head())








