from datetime import datetime, timedelta

from kubernetes.client import models as k8s
from airflow.models import DAG, Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.kubernetes.secret import Secret

#from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
#    KubernetesPodOperator,
#)
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

#CLUSTER_NAME = os.environ.get("KUBERNETES_CLUSTER_NAME")

dag_id = 'kubernetes-dag'

task_default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'depends_on_past': False,
    'execution_timeout': timedelta(hours=1)
}

dag = DAG(
    dag_id=dag_id,
    description='kubernetes pod operator',
    default_args=task_default_args,
    schedule_interval= None,
    max_active_runs=1
)
    
# Use k8s_client.V1ResourceRequirements to define resource limits
k8s_resource_requirements = k8s.V1ResourceRequirements(
    requests={"cpu": "0.2","memory": "100Mi"}, limits={"cpu": "0.5","memory": "512Mi"}
)
        
start = DummyOperator(task_id="start", dag=dag)

run = KubernetesPodOperator(
    task_id="kubernetes-pod-operator",
    namespace='airflow',
    in_cluster=True,
    image='nginx',
    #cluster_name=CLUSTER_NAME,
    #image='ghcr.io/rohminji/batch:master',
    name="db-job",
    is_delete_operator_pod=True,
    kubernetes_conn_id="kubernetes_default",
    # config_file="/home/airflow/composer_kube_config",
    get_logs=True,
    # resources = k8s_resource_requirements,
    dag=dag,
)

start >> run
