from airflow import models
from airflow.operators.python_operator import ShortCircuitOperator, PythonOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.api.common.experimental.trigger_dag import trigger_dag
import datetime
import logging

from PythonFunctions.DailyEventScraper import daily_webscrap

# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

GCP_BUCKET = models.Variable.get('gcp_bucket')
EVENT_FILE_PATH = 'airflow/eventslist/eventslist/eventslist.pickle'
DAG_IDS = {'FOMC Minutes': 'MinutesProcessing'}

DEFAULT_DAG_ARGS = {
    'start_date': YESTERDAY,
    'retries': 0,
    'project_id': models.Variable.get('gcp_project')
}


def check_new_events(df_events):
    '''
    :param kwargs: to extract context (unique_events)
    :return: True if unique events has elements. Otherwise False. This sets up a condition for downstream tasks
    '''
    return not df_events.empty


def create_new_dags(df_events):
    '''
    this function creates new dag runs for the new events
    :param df_events:
    :return:
    '''

    def _create_dag(event):
        if event['title'] == 'FOMC Minutes':
            logging.info('creating dag run for  %s for date %s', event['title'], str(event['date']))
            return trigger_dag(dag_id=DAG_IDS[event['title']],
                        run_id=event['title'] + ',' + str(event['date']),
                        execution_date=event['date'].to_datetime())
    
    df_events.apply(_create_dag, axis=1)


with models.DAG(dag_id='DailyProcessingDAG',
                description='A Dag Run Daily To Scrap FOMC Upcoming Events',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:
    scrapfomcevents = PythonOperator(task_id='scrapfomcevents',
                                     python_callable=daily_webscrap,
                                     op_args=[GCP_BUCKET, EVENT_FILE_PATH],
                                     provide_context=False)

    # get the new events dataframe (returned from the daily_webscrap function as a dataframe).
    df_new_events = "{{ task_instance.xcom_pull(task_ids='scrapfomcevents') }}"

    checknewevents = ShortCircuitOperator(task_id='checknewevents',
                                          python_callable=check_new_events,
                                          op_args=[df_new_events],
                                          provide_context=False)

    createnewdags = PythonOperator(task_id='createnewdags',
                                   python_callable=create_new_dags,
                                   op_args=[df_new_events],
                                   provide_context=False)

    scrapfomcevents >> check_new_events >> createnewdags
