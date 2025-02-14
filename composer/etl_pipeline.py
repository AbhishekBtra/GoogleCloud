import os
from uuid import uuid1
from utility import common
from pipe_logging import log
from airflow.models import DAG
from datetime import datetime,timedelta
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Environment Variables
DTP_PROJECT_ID = os.getenv('dtp_project_id', default=None)
ENVIRONMENT = os.getenv('environment', default=None)
CONFIG_FILES_PARENT_DIR = os.getenv('CONFIG_FILES_PARENT_DIR', default='/home/airflow/gcs')
GCLOUD_LOG_NAME = os.getenv('gcloud_log_name',default=None)


# Variables
workload = 'workload_name'
owner = "gcpcoach7"
job_parallelism_factor = 1
gcloud_log_name = GCLOUD_LOG_NAME
config_dags_file_path=f'{CONFIG_FILES_PARENT_DIR}/data/job-configs/{workload}/schema/tables/pipe'


REGION = "asia-southeast2"
ZONE = "asia-southeast2-c"
LOG_ERROR_STATUS = 'FAILED'
LOG_SUCCESS_STATUS = 'SUCCESS'

# DAG
dag_name=f"{ENVIRONMENT}_etl_{workload}_incremental_pipe"
dag_start_date = days_ago(1)

args = {
    'owner': owner,
    'start_date': dag_start_date
}

dag = DAG(
    dag_id=dag_name,
    schedule_interval= None,#'45 12 * * *',
    start_date=dag_start_date,
    default_args=args,
    catchup=False,
    render_template_as_native_obj=True
)

def etl_process(yaml):

    #read yaml_config

    day_partition_delay_factor = yaml['day_partition_delay_factor']
    gcp_target_bq_project_id = yaml['gcp_target_bq_project_id']
    list_dm_tables = yaml['list_dm_tables']
    list_input_hadoop_tables = yaml['list_input_hadoop_tables']
    list_of_initial_load_sproc = yaml['list_of_initial_load_sproc']
    final_load_sproc =   yaml['final_load_sproc']
    count_of_input_dm_tables = yaml['count_of_input_dm_tables']
    count_of_input_hadoop_tables = yaml['count_of_input_hadoop_tables']
    dm_sql=yaml['dm_sql']
    hadoop_sql=yaml['hadoop_sql']

    #macros
    start_date = f"{{{{ds}}}}"
    partition_process_ds_nodash = f"{{{{ (macros.datetime.strptime(ds,'%Y-%m-%d') - macros.timedelta(days={day_partition_delay_factor})).strftime('%Y%m%d') }}}}"
    partition_process_dt = f"{{{{ (macros.datetime.strptime(ds,'%Y-%m-%d') - macros.timedelta(days={day_partition_delay_factor})).strftime('%Y-%m-%d 00:00:00') }}}}"
    bq_load_id = f"{dag_name}_{start_date}"

    #prepare guid
    guid_str = uuid1()
    #read guid from xcom
    guid = f"""{{{{ti.xcom_pull(task_ids='Etl_workload_name.task_prepare_guid')}}}}"""

    #set project ids for project other than data-dtp
    if ENVIRONMENT == 'dev':
        data_bi_project_id ='bi-dev-e36d'
        data_analytics_project_id='dev-e36d'

    elif ENVIRONMENT =='prd':
        data_bi_project_id='bi-prd-935c'
        data_analytics_project_id='prd-1b95'

    start_etl = BashOperator(
                task_id='start_etl',
                bash_command=f"echo {partition_process_dt}"
            )
    
    #list of bq insert job operator
    list_bq_insert_job_operator=[]

    #final_sproc
    final_sproc = f"{final_load_sproc}".replace('<schema_name>.','')

    #remove already present parts from dm tables
    #this is necessary to ensure idempotency
    bq_partition_removal_bash_command= \
        common.prepare_bq_partition_removal_bash_command(
                                list_dm_tables,
                                DTP_PROJECT_ID,
                                partition_process_ds_nodash
                                )
    
    task_prepare_guid = BashOperator(
                task_id='task_prepare_guid',
                bash_command=f"echo {guid_str}"
            )
    
    task_dm_remove_partitions = BashOperator(
                task_id='task_dm_remove_partitions_if_exist',
                bash_command=bq_partition_removal_bash_command,
                on_failure_callback=log.write_fail_logs
            )
    
    task_check_required_input_hdp_tables_loaded = PythonOperator(
        task_id = f"task_check_required_input_hdp_tables_loaded",
        python_callable=common.check_hadoop_input_table_count,
        op_kwargs={
            'count':count_of_input_hadoop_tables,
            'sql':hadoop_sql,
            'dtp_project_id':DTP_PROJECT_ID,
            'region':REGION,
            'process_dt_nodash':partition_process_ds_nodash,
            'data_analytics_project_id':data_analytics_project_id,
            'data_bi_project_id':data_bi_project_id
        },
        on_failure_callback=log.write_fail_logs
    )

    task_check_required_input_dm_tables_loaded = PythonOperator(
        task_id = f"task_check_required_input_dm_tables_loaded",
        python_callable=common.check_dm_input_table_loaded_count,
        op_kwargs={
            'count':count_of_input_dm_tables,
            'sql':dm_sql,
            'dtp_project_id':DTP_PROJECT_ID,
            'region':REGION,
            'process_dt_nodash':partition_process_ds_nodash
        },
        on_failure_callback=log.write_fail_logs
    )

    with TaskGroup('Transformations') as transformations:
    
        list_bq_insert_job_operator = common.prepare_transformations_operator(
            dag=dag,
            project_id=DTP_PROJECT_ID,
            region=REGION,
            list_of_initial_load_sproc=list_of_initial_load_sproc,
            bq_load_id=bq_load_id,
            guid=guid,
            partition_process_dt=partition_process_dt
        )

        list_bq_insert_job_operator

    task_final_load = BigQueryInsertJobOperator(
        task_id=f'task_final_load_run_{final_sproc}',
        configuration={
            "query": {
                "query": f"CALL {final_load_sproc}('{bq_load_id}','{partition_process_dt}','{guid}') ",
                "useLegacySql": False,
            }
        },
        on_failure_callback=log.write_fail_logs,
        location=REGION,
        project_id=DTP_PROJECT_ID
    )


    task_write_etl_sql_logs_to_cloud = PythonOperator(

        task_id=f"task_write_etl_sql_logs_to_cloud",
        python_callable=common.write_etl_sql_logs_to_cloud,
        op_kwargs={
            'dtp_project_id':DTP_PROJECT_ID,
            'region':REGION,
            'bq_load_id':bq_load_id,
            'guid':guid,
            'gcloud_log_name':gcloud_log_name,
            'dag_name':dag_name,
            'workload':workload
        },
        on_failure_callback=log.write_fail_logs,
        trigger_rule=TriggerRule.ALL_DONE
    )

    end_etl = BashOperator(
                task_id='end_etl',
                bash_command=f"echo {partition_process_dt}",
                trigger_rule=TriggerRule.ALL_DONE
            )

    start_etl >> task_prepare_guid>> task_check_required_input_hdp_tables_loaded >> task_dm_remove_partitions >> transformations >>task_check_required_input_dm_tables_loaded  >> task_final_load >> task_write_etl_sql_logs_to_cloud >> end_etl


def main():

    from yaml import safe_load

    start_bash_cmd = log.prepare_log_bash_command(
                    gcloud_log_name=gcloud_log_name,
                    dag_name=dag_name,
                    workload=workload,
                    action='START_JOB',
                    message=f"Started {workload} Incremental Pipe {dag_name}",
                    status=f'{LOG_SUCCESS_STATUS}',
                    severity='INFO'
                )

    end_bash_cmd = log.prepare_log_bash_command(
                    gcloud_log_name=gcloud_log_name,
                    dag_name=dag_name,
                    workload=workload,
                    action='END_JOB',
                    message=f"Ended {workload} Incremental Pipe {dag_name}",
                    status=f'{LOG_SUCCESS_STATUS}',
                    severity='INFO'
                )

    with dag:
         

        task_etl_incremental_pipe_start = BashOperator(
                task_id='task_etl_incremental_pipe_start',
                bash_command=start_bash_cmd,
                dag=dag
            )
    
        task_etl_incremental_pipe_end = BashOperator(
                task_id='task_etl_incremental_pipe_end',
                bash_command=end_bash_cmd,
                dag=dag,
                trigger_rule=TriggerRule.ALL_DONE
            )

        dag_config_files = os.listdir(config_dags_file_path)

        #lists for parallelism
        list_table_task_group = []
        list_view_table_task_group_in_parallel = []
        for i in range(job_parallelism_factor):
            list_view_table_task_group_in_parallel.append(task_etl_incremental_pipe_start)

        for dag_config_file in dag_config_files:
                if dag_config_file.endswith(".yaml") :
                    with open(f"{config_dags_file_path}/{dag_config_file}", 'r') as file:
                        #data = file.read()
                        yaml_data = safe_load(file)
                        
                    
                    # Generating tasks from config files
                    with TaskGroup(group_id='Etl_workload_name') as etl_task_grp:
                        # Generating tasks from config files
                        etl_process(yaml_data)
                        list_table_task_group.append(etl_task_grp)
                        

                else:
                    continue
        
        #update view list
        for order,table_task_group in enumerate(list_table_task_group): 
            idx = order % job_parallelism_factor
            list_view_table_task_group_in_parallel[idx] = list_view_table_task_group_in_parallel[idx] >> table_task_group

        for view_table_task_group_in_parallel in list_view_table_task_group_in_parallel:
            view_table_task_group_in_parallel >> task_etl_incremental_pipe_end

main()