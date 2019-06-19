import numpy as np
import pandas as pd
import datetime as dt
import os

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


def find_all_file_paths(path):
    print('Collecting all file paths...')
    files = []
    for r, d, f in os.walk(path):
        for file in f:
            if 'tripdata' in file:
                files.append(os.path.join(r, file))

    print('File paths collected.')
    return files


def load_and_clean_files(files):

    print('Loading, appending, and cleaning files...')
    all_raw_data = pd.DataFrame()

    for file in files:
        raw_data = load_file(file)
        all_raw_data = append_raw_data(raw_data, all_raw_data)

    clean_data = clean_raw_data(all_raw_data)
    print('Files loaded, appended, and cleaned.')
    return clean_data


def load_file(file_path):
    print('Loading...')

    def converter_function(value):
        if value == '':
            return np.nan
        return int(value)

    if 'fhv' in file_path:
        return pd.read_csv(file_path,
                           parse_dates=[0, 1],
                           infer_datetime_format=True,
                           usecols=[0, 1, 2, 3],
                           names=['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location'],
                           converters={2: converter_function, 3: converter_function},
                           header=0)

    elif 'yellow' in file_path:
        return pd.read_csv(file_path,
                           parse_dates=[0, 1],
                           infer_datetime_format=True,
                           usecols=[1, 2, 7, 8],
                           names=['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location'],
                           converters={7: converter_function, 8: converter_function},
                           header=0)

    elif 'green' in file_path:
        return pd.read_csv(file_path,
                           parse_dates=[0, 1],
                           infer_datetime_format=True,
                           usecols=[1, 2, 5, 6],
                           names=['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location'],
                           converters={5: converter_function, 6: converter_function},
                           header=0)
    else:
        print('XXX raw data error')


def clean_raw_data(data):
    print('Cleaning...')
    data.dropna(inplace=True)
    data.drop_duplicates(inplace=True)
    data['JourneyTime'] = [journey_time.seconds for journey_time in (data['DO_Datetime'] - data['PU_Datetime'])]
    data['Link'] = [str({int(x), int(y)}) for x, y in zip(data['PU_Location'], data['DO_Location'])]

    data['PU_Datetime'] = data['PU_Datetime'].apply(lambda x: (x - dt.datetime(x.year, x.month, x.day)).seconds)
    bins = np.digitize(data['PU_Datetime'].values, bins=time_bins)
    data['TimeCats'] = time_cats[bins]
    data = (data.drop(columns=data.columns[0:4]))
    print('Cleaned.')
    return data


def append_raw_data(new_data, all_data):
    print('Appending data...')
    return pd.concat([new_data, all_data], ignore_index=True)


def save_clean_data(data):
    print('Saving data...')
    data.to_csv(path_to_files + '/cleaned_data.csv')


time_bins = [8*60*60, 10*60*60, 16*60*60, 18*60*60, 24*60*60]
time_cats = np.array(['Morning', 'MorningRush', 'Day', 'AfternoonRush', 'Night'])
path_to_files = 'C:/Users/medkmin/PycharmProjects/TransportData/Multiple'

file_paths = find_all_file_paths(path_to_files)
cleaned_data = load_and_clean_files(file_paths)
save_clean_data(cleaned_data)

