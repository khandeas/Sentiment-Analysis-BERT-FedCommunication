from airflow import configuration
from airflow import models
from airflow.contrib.operators import dataflow_operator
from airflow.operators import python_operator
from airflow.utils.trigger_rule import TriggerRule
import datetime
import logging

from PythonFunctions.FedMinutesDownload import download_minutes

# We set the start_date of the DAG to the previous date. This will
# make the DAG immediately available for scheduling.
YESTERDAY = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())

# We define some variables that we will use in the DAG tasks.
SUCCESS_TAG = 'success'
FAILURE_TAG = 'failure'

# The following additional Airflow variables should be set:
# gcp_project:         Google Cloud Platform project id.
# gcp_temp_location:   Google Cloud Storage location to use for Dataflow temp location.
# email:               Email address to send failure notifications.

DEFAULT_DAG_ARGS = {
    'start_date': YESTERDAY,
    'retries': 0,
    'project_id': models.Variable.get('gcp_project'),
    'dataflow_default_options': {
        'project': models.Variable.get('gcp_project'),
        'temp_location': models.Variable.get('gcp_temp_location'),
        'staging_location': models.Variable.get('gcp_staging_location'),
        'runner': 'DataflowRunner'
    }
}


TARGET_EVENT = '{{ dag_run.conf["event_date"] }}'
GCP_BUCKET = models.Variable.get('gcp_bucket')
DATAFLOW_MINUTES_TEMPLATE = models.Variable.get('gcp_dataflow_template_location') + '/dailyminutesprocessing'
OUTPUT_FILE_PATH = 'gs://' + models.Variable.get('gcp_bucket') + '/' + 'dataflow/processed/processedminutes'


def task_completion(status, **kwargs):
    if status == SUCCESS_TAG:
        logging.info('successfully processed minutes document %s', kwargs['dag_run'].conf['event_date'])
    else:
        logging.info('failure in processing minutes document', kwargs['dag_run'].conf['event_date'])


with models.DAG(dag_id='MinutesProcessing',
                description='A Dag Triggering Minutes Processing Job',
                schedule_interval=None, default_args=DEFAULT_DAG_ARGS) as dag:
    # Args required for the Dataflow job.

    downloadminutes = python_operator.PythonOperator(task_id='downloadminutes',
                                                     python_callable=download_minutes,
                                                     op_args=[GCP_BUCKET, TARGET_EVENT],
                                                     provide_context=True)

    # use template for the xcom
    job_args = {
        'input': "{{ task_instance.xcom_pull(task_ids='downloadminutes') }}",
        'output': OUTPUT_FILE_PATH
    }

    dataflow_task = dataflow_operator.DataflowTemplateOperator(
        template=DATAFLOW_MINUTES_TEMPLATE,
        task_id="processminutes",
        parameters=job_args)

    # Here we create two conditional tasks, one of which will be executed
    # based on whether the dataflow_task was a success or a failure.
    success_move_task = python_operator.PythonOperator(task_id='success-completion',
                                                       python_callable=task_completion,
                                                       op_args=[SUCCESS_TAG],
                                                       provide_context=True,
                                                       trigger_rule=TriggerRule.ALL_SUCCESS)

    failure_move_task = python_operator.PythonOperator(task_id='failure-completion',
                                                       python_callable=task_completion,
                                                       op_args=[FAILURE_TAG],
                                                       provide_context=True,
                                                       trigger_rule=TriggerRule.ALL_FAILED)

    # The success_move_task and failure_move_task are both downstream from the
    # dataflow_task.
    downloadminutes >> dataflow_task >> success_move_task
    downloadminutes >> dataflow_task >> failure_move_task
