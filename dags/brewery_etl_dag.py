"""
brewery_etl_dag.py
"""

# Importing the Libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.email import send_email
from airflow.operators.bash import BashOperator


# Setting the emails to receive alerts
recipients = ['aldenis@gmail.com']

def failure_callback(context):
    """
    Callback function that will be executed when the task fails.
    """
    subject = f"Airflow Alert: A tarefa {context['task_instance'].task_id} falhou!"
    html_content = f"""
    <p>Olá,</p>
    <p>A execução da DAG <b>{context['dag'].dag_id}</b> falhou.</p>
    <p>Detalhes da falha:</p>
    <ul>
      <li><b>Tarefa:</b> {context['task_instance'].task_id}</li>
      <li><b>Execução ID:</b> {context['dag_run'].run_id}</li>
      <li><b>Data da Execução:</b> {context['execution_date']}</li>
      <li><b>Log:</b> <a href="{context['task_instance'].log_url}">Ver Logs</a></li>
    </ul>
    """
    send_email(to=recipients, subject=subject, html_content=html_content)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,  # enable sending email in case of failure
    'email': recipients,
    'email_on_retry': False,
    'retries': 1,
	'retry_delay': timedelta(minutes=1),
    'on_failure_callback': failure_callback,  # uses the callback function
}


# Building the DAG
with DAG(
	'brewery_etl_dag',
	default_args=default_args,		
	description= 'Applying ETL in consumed data from an API about World Breweries',
	schedule= '0 0 * * *', # everyday at 00:00
	catchup= False,
	tags= ['extract_bronze','transform_silver', 'run_transform_tests', 'run_quality_tests','load_gold','run_data_analysis'],
	) as dag:
		extract_bronze = BashOperator(
			task_id='extract_bronze',
			bash_command='python3 ../scripts/extract.py >> /opt/airflow/logs/extract.log 2>&1',
		)
		transform_silver = BashOperator(
			task_id='transform_silver',
			bash_command='python3 ../scripts/transform.py >> /opt/airflow/logs/transform.log 2>&1',
		)
		run_transform_tests = BashOperator(
			task_id='run_transform_tests',
			bash_command='pytest ../tests/test_transform.py >> /opt/airflow/logs/test_transform.log 2>&1',
		)
		run_quality_tests = BashOperator(
			task_id='run_quality_tests',
			bash_command='python3 ../scripts/quality.py >> /opt/airflow/logs/quality.log 2>&1',
		)
		load_gold = BashOperator(
			task_id='load_gold',
			bash_command='python3 ../scripts/load.py >> /opt/airflow/logs/load.log 2>&1',
		)
		run_data_analysis = BashOperator(
			task_id='run_data_analysis',
			bash_command='python3 ../scripts/data_analysis.py >> /opt/airflow/logs/data_analysis.log 2>&1',
		)
  
		extract_bronze >> transform_silver >> run_transform_tests >> run_quality_tests >> load_gold >> run_data_analysis