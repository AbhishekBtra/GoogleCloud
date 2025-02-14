from pipe_logging import log
from airflow.operators.dummy import DummyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

def prepare_transformations_operator(dag,project_id,region,list_of_initial_load_sproc,bq_load_id,guid,partition_process_dt):

    
    list_of_bq_insert_job_operator = []
    list_display_bq_insert_job_operator = []
    query_template = f"CALL dm.$dm_prereq_data_load_sproc ('$bq_load_id','$partition_process_dt','$guid') "
    configuration_template={
            "query": {
                "query": "",
                "useLegacySql": False,
            }
        }

    for val in list_of_initial_load_sproc:

        dm_prereq_data_load_sproc = val.replace('dm.','')
        modified_query = query_template.replace('$dm_prereq_data_load_sproc',f"{dm_prereq_data_load_sproc}")\
        .replace('$bq_load_id',str(bq_load_id))\
        .replace('$partition_process_dt',str(partition_process_dt))\
        .replace('$guid',str(guid))

        bq_insert_job_oprtr = BigQueryInsertJobOperator(
        task_id=f'task_load_prereq_dm_tables_run_$dm_prereq_data_load_sproc'\
            .replace('$dm_prereq_data_load_sproc',str(dm_prereq_data_load_sproc)),
        configuration={
            "query": {
                "query": "",
                "useLegacySql": False,
            }
        },
        on_failure_callback=log.write_fail_logs,
        location=region,
        project_id=project_id,
        dag=dag
        )
        
        bq_insert_job_oprtr.configuration['query']['query'] = modified_query

        list_of_bq_insert_job_operator.append(bq_insert_job_oprtr)

    list_display_bq_insert_job_operator.append(list_of_bq_insert_job_operator[0])
    
    for idx in range(1,len(list_of_bq_insert_job_operator)):

        list_display_bq_insert_job_operator[0] = list_display_bq_insert_job_operator[0] >> list_of_bq_insert_job_operator[idx]
    
    return list_display_bq_insert_job_operator






    





