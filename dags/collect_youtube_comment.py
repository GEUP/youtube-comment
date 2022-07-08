import airflow
from airflow import DAG
from airflow.utils.log.logging_mixin import LoggingMixin
import pendulum
from googleapiclient.discovery import build
import pendulum
import os

from custom_component.operators import (
    RabbitMQOperator,
    CollectYoutubeVideoIDOperator,
    CollectNotExistYoutubeVideoCommentsOperator,
    CollectExistYoutubeVideoCommentsOperator,
)


dag = DAG(
    dag_id="collect_youtube_comment",
    start_date=pendulum.now().subtract(days=2),
    schedule_interval="@daily",
    default_args={"output_path": "/comments"},
)

collect_video_id = CollectYoutubeVideoIDOperator(
    task_id="collect_video_id", conn_id="youtube_data_api", dag=dag
)

pub_exist_video_id = RabbitMQOperator(
    task_id="pub_exist_video_id",
    conn_id="rabbitmq_video_id",
    xcom_key="exist_video_id_list",
    routing_key="exist_video_q",
    dag=dag,
)

pub_not_exist_video_id = RabbitMQOperator(
    task_id="pub_not_exist_video_id",
    conn_id="rabbitmq_video_id",
    xcom_key="not_exist_video_id_list",
    routing_key="not_exist_video_q",
    dag=dag,
)


collect_not_exist_video_comment = CollectNotExistYoutubeVideoCommentsOperator(
    task_id="collect_not_exist_video_comment",
    youtube_data_api_conn_id="youtube_data_api",
    rabbitmq_conn_id="rabbitmq_video_id",
    queue_name="not_exist_video_q",
    dag=dag,
)

collect_exist_video_comment = CollectExistYoutubeVideoCommentsOperator(
    task_id="collect_exist_video_comment",
    youtube_data_api_conn_id="youtube_data_api",
    rabbitmq_conn_id="rabbitmq_video_id",
    queue_name="exist_video_q",
    dag=dag,
)

collect_video_id >> pub_exist_video_id
collect_video_id >> pub_not_exist_video_id
pub_not_exist_video_id >> collect_not_exist_video_comment
pub_exist_video_id >> collect_exist_video_comment
