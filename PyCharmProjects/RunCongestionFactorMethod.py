import pandas as pd
import numpy as np


def calculate_average_journey_time(data):
    print('Calculating average journey time...')
    average_time = data.groupby(['Link', 'TimeCats']).mean().unstack('TimeCats')
    average_time.columns = time_cats
    print('Average journey time calculated.')
    return average_time


def calculate_if_journey_congested(clean_data, average_data, congestion_factor):
    print('Running congestion method...')
    clean_data['IsCongested'] = [clean_data.iloc[index, 0] >
                                 congestion_factor * average_data.loc[clean_data.iloc[index, 1], clean_data.iloc[index, 2]]
                                 for index in range(clean_data.shape[0]) if not np.isnan(index)]
    # c_data['CongestionTime'] = [congestion_factor * h_data.loc[c_data.iloc[index, 1], c_data.iloc[index, 2]]
    #                             for index in range(c_data.shape[0])]
    print('Congestion method complete.')
    return clean_data


# load cleaned data
time_bins = [8*60*60, 10*60*60, 16*60*60, 18*60*60, 24*60*60]
time_cats = np.array(['Morning', 'MorningRush', 'Day', 'AfternoonRush', 'Night'])
path_to_files = 'C:/Users/medkmin/PycharmProjects/TransportData/Multiple'

cleaned_data = pd.read_csv(path_to_files + '/cleaned_data.csv', usecols=[1, 2, 3])
historical_data = calculate_average_journey_time(cleaned_data)
congested_data = calculate_if_journey_congested(cleaned_data, historical_data, congestion_factor=100)
print(congested_data[congested_data['IsCongested']].groupby('TimeCats').count())

# plot results

congested_data.to_csv(path_to_files + '/congestion_data.csv')


