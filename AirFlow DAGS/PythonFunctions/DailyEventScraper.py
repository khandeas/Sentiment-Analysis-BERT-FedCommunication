import pandas as pd
import numpy as np
from urllib.request import urlopen
import json
import datetime
import re
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

# Global Variables
FOMC_CALENDAR_URL = 'https://www.federalreserve.gov/json/calendar.json'
re_tags = re.compile(r'(&lt;[//]?p|&gt;|&lt;br.*)')


def daily_webscrap(gcp_bucket, gs_path_eventslist):
    '''
    this function scraps the fomc calendar .json file to get events lists. the extracted events list is then compared
    previous events list stored in the gcp bucket
    :param gcp_bucket: gcp bucket where events list pickle file is stored
    :param gs_path_eventslist: relative path of the pickled file in the bucket
    :return:
    '''

    # read the data from url
    def filter_records(events):
        '''
        This function strips out relavent event details from
        each row in the dataframe
        '''

        # extract date and time
        current_date = datetime.datetime.now()
        event_datestr = events['days'] + '-' + events['month'] + ',' \
                        + events['time'].replace(' p.m.', 'PM').replace(' a.m.', 'AM')
        event_datetime = datetime.datetime.strptime(event_datestr, '%d-%Y-%m,%I:%M%p')

        # if the event is for the future
        if event_datetime > current_date:
            try:
                filtered_description = re.sub(re_tags, '', events['description'])
            except:
                filtered_description = str(events['title']) + ' ' + str(event_datetime.date())

            final_record = {'date': event_datetime, 'description': filtered_description, \
                            'title': events['title'], 'category': events['type']}
            return final_record
        else:
            return np.nan

    with urlopen(FOMC_CALENDAR_URL) as url:
        data = json.loads(url.read())
    # convert the data to dataframe for easy manipulation
    df_events = pd.DataFrame(data['events'])

    # filter by type - 'FOMC','Speeches','Beige'
    type_filter = ['FOMC', 'Beige', 'Speeches']
    df_filtered = df_events[df_events.type.isin(type_filter)]

    # apply the filter records function in parallel. get the results
    df_result = df_filtered.apply(filter_records, axis=1)

    # create the final data frame.
    df_final = pd.DataFrame(list(df_result.dropna().values))

    # create a hash using title + date to create an index
    df_final['indexhash'] = (df_final['title'] + ', ' + df_final['date'].apply(lambda x: str(x.date()))).apply(
        lambda x: hash(x))
    df_final.set_index('indexhash', drop=True)
    return (compare_events(df_final, gcp_bucket, gs_path_eventslist))


def compare_events(df_final, gcp_bucket, gs_path_eventslist):
    '''
    this function compares the list of events scrapped from web with existing one stored in cloud storage bucket. any new events
    will have new dags scheduled accordingly. The events stored in the storage will also be updated if updates did occur.
    :param df_final: dataframe with list of events scrapped from web
    :param gcp_bucket: this is the gcs bucket where the events list file is stored
    :param gs_path_eventslist: this is the (relative) path to the events file in the cloud storage bucket
    :return:
    '''

    # establish connection with cloudstorage bucket using default Airflow Settings
    conn = GoogleCloudStorageHook()
    prev_events_file = conn.download(bucket=gcp_bucket, object=gs_path_eventslist)

    # reconstruct dataframe
    df_previous_events = pd.read_pickle(prev_events_file)

    # find out the unique rows in the latest index
    unique_events = set(df_final.index) - set(df_previous_events.index)

    # return unique events
    return df_final.loc[unique_events]
