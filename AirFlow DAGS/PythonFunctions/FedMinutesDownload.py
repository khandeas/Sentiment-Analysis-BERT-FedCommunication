import re
import json
import pandas as pd
import datetime
import numpy as np
import logging
from bs4 import BeautifulSoup
from urllib.request import urlopen
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.exceptions import AirflowException

# Global Variables

FED_BASE_URL = 'https://www.federalreserve.gov'
FED_EVENTS_SOURCE = 'https://www.federalreserve.gov/json/ne-press.json'
RENAME_COLUMNS = {'d': 'date', 'l': 'link', 'pt': 'categ', 'pt2': 'othercateg', 'stub': 'stub', 't': 'title',
                  'updateDate': 'updateddate'}


# extender class for gcshook to allow for upload using string

class GCSHookExtender(GoogleCloudStorageHook):
    # call the base init function
    def __init__(self,
                 google_cloud_storage_conn_id='google_cloud_default',
                 delegate_to=None):
        super(GoogleCloudStorageHook, self).__init__(google_cloud_storage_conn_id,
                                                     delegate_to)

    def upload_from_str(self, bucket, object, filestr,
                        mime_type='application/octet-stream'):
        """
        Uploads a local file to Google Cloud Storage.

        :param bucket: The bucket to upload to.
        :type bucket: str
        :param object: The object name to set when uploading the local file.
        :type object: str
        :param filestr: The string content to upload.
        :type filestr: str
        :param mime_type: The MIME type to set when uploading the file.
        :type mime_type: str
         """

        client = self.get_conn()
        bucket = client.bucket(bucket)
        blob = bucket.blob(blob_name=object)
        blob.upload_from_string(filestr,
                                content_type=mime_type)

        self.log.info('File %s uploaded to %s in %s bucket', filestr, object, bucket)


def download_minutes(gcp_bucket, target_event, **kwargs):
    # Formatting function used for dates. Specific to minutes data
    def format_date(x):
        try:
            return datetime.datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')
            # return date_fmt
        except ValueError as e:
            # the time element may be absent. try another format
            try:
                return datetime.datetime.strptime(x, '%m/%d/%Y')
            except:
                return np.nan

    logging.info('received variables bucket is %s and event is %s', gcp_bucket, target_event)

    # Read the Events Source
    with urlopen(FED_EVENTS_SOURCE) as url:
        events_data = json.loads(url.read())

    # rename columns
    df_events = pd.DataFrame(events_data).rename(columns=RENAME_COLUMNS)
    # drop na dates. cause issues with applymap funcitons
    df_events.dropna(axis=0, subset=['date'], inplace=True)

    # format the dates
    df_events['date'] = df_events[['date']].applymap(lambda x: format_date(x))

    # Extract the relevant event link
    try:
        event_link = df_events[df_events['date'] == pd.Timestamp(target_event)]['link']
        # contructed event url
        event_url = FED_BASE_URL + event_link.values[0]

        # open the url. scrap through beautiful soup to get url
        event_page = urlopen(event_url)
        bs_event_contents = BeautifulSoup(event_page, 'html.parser')

        minutes_link = bs_event_contents.find('a', href=re.compile('^/monetarypolicy/fomcminutes\d{8}.htm'))
        minutes_url = FED_BASE_URL + minutes_link.attrs['href']

        # extract meeting date from link
        re_date_url = re.compile(r'(\d{8}).htm[l]?')
        mt_dt = re.search(re_date_url, minutes_url)
        if mt_dt:
            datestr = mt_dt.group(1)
            meeting_dt = datetime.datetime.strptime(datestr, '%Y%m%d')
        else:
            meeting_dt = datetime.datetime.now()

        # Download the Data to Google Drive

        conn = GCSHookExtender()
        minuteslink = urlopen(minutes_url)
        fname = "{}{}{}.html".format(meeting_dt.year, meeting_dt.month, meeting_dt.day)
        gcs_filename = 'raw/minutes/' + str(meeting_dt.year) + '/' + fname
        conn.upload_from_str(gcp_bucket, gcs_filename, minuteslink.read())
        gcp_full_filename = 'gs://' + gcp_bucket + '/' + gcs_filename
        return gcp_full_filename

    except:

        raise AirflowException("Event Specified is not in the Feds Event List")
