import numpy as np
import pandas as pd
import datetime as dt
import os

# todo check code is saving properly
# todo implement percentile factor method

# Use import cProfile
#
# import cProfile
# pr = cProfile.Profile()
# pr.enable()
# your_function_call()
# pr.disable()
# # after your program ends
# pr.print_stats(sort="calls")

# to profile each method

time_bins = [8*60*60, 10*60*60, 16*60*60, 18*60*60, 24*60*60]
time_cats = np.array(['Morning', 'MorningRush', 'Day', 'AfternoonRush', 'Night'])


def find_individual_raw_data():
    path = 'M:/GitHub/Anomaly-Detection/Individual/yellow_tripdata_2018-01.csv'
    return pd.read_csv(path)


def find_multiple_raw_data():
    print('Getting multiple raw data files...')
    path = 'C:/Users/medkmin/PycharmProjects/TransportData/Multiple'

    files = []
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(r, file))

    column_names = ['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location']
    data = pd.DataFrame()

    for file in files:

        def converter_function(value):
            if value == '':
                return -1
            return int(value)

        if 'fhv' in file:
            print('Reading data "fhv"...')
            file_data = pd.read_csv(file,
                                    parse_dates=[0, 1],
                                    infer_datetime_format=True,
                                    usecols=[0, 1, 2, 3],
                                    names=column_names,
                                    converters={2: converter_function, 3: converter_function},
                                    header=0)
            print('Data read.')
            data = pd.concat([data, clean_raw_data(file_data)], ignore_index=True)

        elif 'yellow' in file:
            print('Reading data "yellow"...')
            file_data = pd.read_csv(file,
                                    parse_dates=[0, 1],
                                    infer_datetime_format=True,
                                    usecols=[1, 2, 7, 8],
                                    names=column_names,
                                    converters={7: converter_function, 8: converter_function},
                                    header=0)
            print('Data read.')
            data = pd.concat([data, clean_raw_data(file_data)], ignore_index=True)

        elif 'green' in file:
            print('Reading data "green"...')
            file_data = pd.read_csv(file,
                                    parse_dates=[0, 1],
                                    infer_datetime_format=True,
                                    usecols=[1, 2, 5, 6],
                                    names=column_names,
                                    converters={5: converter_function, 6: converter_function},
                                    header=0)
            print('Data read.')
            data = pd.concat([data, clean_raw_data(file_data)], ignore_index=True)

        else:
            print('raw data error')

    return data


def clean_raw_data(data):
    print('Cleaning data...')

    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data['JourneyTime'] = [journey_time.seconds for journey_time in (data['DO_Datetime'] - data['PU_Datetime'])]
    data['Link'] = [str({x, y}) for x, y in zip(data['PU_Location'], data['DO_Location'])]

    data['PU_Datetime'] = data['PU_Datetime'].apply(lambda x: (x - dt.datetime(x.year, x.month, x.day)).seconds)
    bins = np.digitize(data['PU_Datetime'].values, bins=time_bins)
    data['TimeCats'] = time_cats[bins]
    data = (data.drop(columns=data.columns[0:4]))
    print('Data cleaned.')
    return data


def calculate_average_journey_time(data):
    print('Calculating average journey time...')
    average_time = data.groupby(['Link', 'TimeCats']).mean().unstack('TimeCats')
    average_time.columns = time_cats
    print('Average journey time calculated.')
    return average_time


def run_congestion_factor_method(c_data, h_data, congestion_factor):
    print('Running congestion method...')

# todo check values of h_data and c_data. Getting runtime errors

    c_data['IsCongested'] = [c_data.iloc[index, 0] >
                             congestion_factor * h_data.loc[c_data.iloc[index, 1], c_data.iloc[index, 2]]
                             for index in range(c_data.shape[0])]
    c_data['CongestionTime'] = [congestion_factor * h_data.loc[c_data.iloc[index, 1], c_data.iloc[index, 2]]
                                for index in range(c_data.shape[0])]
    print('Congestion method complete.')
    return c_data


# raw_data = find_individual_raw_data()
# print(raw_data.head())
# clean_data = clean_raw_data(raw_data.iloc[:100, :], 'yellow')

clean_data = find_multiple_raw_data()
# print(clean_data)
historical_data = calculate_average_journey_time(clean_data)
# print(historical_data)
congested_data = run_congestion_factor_method(clean_data, historical_data, congestion_factor=100)
print(congested_data[congested_data['IsCongested']])
congested_data.to_csv('M:/GitHub/Anomaly-Detection/PyCharmProjects')


