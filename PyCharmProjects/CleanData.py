import numpy as np
import pandas as pd
import datetime as dt
import math
import os

# todo check code for duplicates
# todo add gitignore for extra files


time_categories = pd.DataFrame(data=[[dt.time.min, dt.time(8)],
                                     [dt.time(8), dt.time(10)],
                                     [dt.time(10), dt.time(16)],
                                     [dt.time(16), dt.time(18)],
                                     [dt.time(18), dt.time.max]],
                               index=['Morning', 'MorningRush', 'Day', 'AfternoonRush', 'Night'],
                               columns=['StartTime', 'EndTime'])


def find_raw_data():
    path = 'M:/GitHub/Anomaly-Detection/Small/yellow_tripdata_2018-01.csv'
    return pd.read_csv(path)

    # Uncomment when using more than one file
    #
    # files = []
    # for r, d, f in os.walk(path):
    #     for file in f:
    #         if '.csv' in file:
    #             files.append(os.path.join(r, file))
    #
    # for f in files:
    # add appropriate columns from each file to concantenated full dataframe
    # raw_data = pd.concat([raw_data, file], ignore_index=True)

    # determine type of each file
    #
    # if 'fhv' in f:
    #     file = (file.drop(columns=file.columns[4:]))
    # elif 'yellow' in f:
    #     file = (file.drop(columns=file.columns[0])
    #                 .drop(columns=file.columns[3:7])
    #                 .drop(columns=file.columns[9:]))
    # else:
    #     file = (file.drop(columns=file.columns[0])
    #             .drop(columns=file.columns[3:5])
    #             .drop(columns=file.columns[7:]))


def clean_raw_data(data):

    data = (data.drop(columns=data.columns[0])
            .drop(columns=data.columns[3:7])
            .drop(columns=data.columns[9:]))

    data.columns = ['PU_Datetime', 'DO_Datetime', 'PU_Location', 'DO_Location']
    data.dropna(inplace=True)
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


# def calculate_average_journey_time(data):

# def calculate_average_journey_time_by_link(current_data):
#     average_journey_time_by_category = pd.Series(index=time_categories.index)
#     for category in time_categories.index:
#         if ([journey_time for journey_time in
#              current_data[current_data['TimeCategory'] == category]['JourneyTime']]):
#             mean = (np.mean([journey_time for journey_time
#                              in current_data[current_data['TimeCategory'] == category]['JourneyTime']]))
#             average_journey_time_by_category[category] = mean
#         else:
#             average_journey_time_by_category[category] = 0
#     # print()
#     # print('current data')
#     # print(current_data)
#     # print()
#     # print('average_journey_time')
#     # print(average_journey_time_by_category)
#     # print()
#     return average_journey_time_by_category
# #     return pd.Series(index=time_categories.index)

    # return average_time_by_category


raw_data = find_raw_data()
# print(raw_data.head())
clean_data = clean_raw_data(raw_data.iloc[:, :])
# print(clean_data)
# clean_data['Jou']

# convert journeytime to seconds


# average_time_by_category = (clean_data.groupby(['Link']).apply(calculate_average_journey_time_by_link))
# print(average_time_by_category)

# average_journey_time = calculate_average_journey_time(clean_data)
# print(average_journey_time)

# link_index = clean_data.groupby(['Link']).count().index
# df = pd.DataFrame(index=link_index, columns=time_categories.index)
# print(df)
df = clean_data.groupby(['Link', 'TimeCategory']).mean()
print(df)
# print(average_journey_time.head(), average_journey_time.shape, average_journey_time.index, sep='\n')







