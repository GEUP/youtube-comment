import airflow
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from kubernetes.client import models as k8s
import pendulum
import json

volume_claim = k8s.V1PersistentVolumeClaimVolumeSource(
    claim_name = "comments-volume"
)
volume = k8s.V1Volume(
    name="comments-volume",
    persistent_volume_claim=volume_claim
)
volume_mount = k8s.V1VolumeMount(
    name="comments-volume",
    mount_path="/comments", #컨테이너 안에서 볼륨을 마운트할 경로
    read_only=False
)

output_path = "/commnets"

dag = DAG(
    dag_id="collect_youtube_comment_in_container",
    start_date=pendulum.now().subtract(days=2),
    schedule_interval="@daily",
    user_defined_filters={"from_str_to_json": lambda s: json.loads(s)}
)

collect_video_id = KubernetesPodOperator(
    task_id="collect_video_id", 
    image="geup/collect_video_id:test",
    #cmds=[""],
    arguments=[
        "--output_path", output_path,
        "--token", "{{ conn.youtube_data_api.password }}",
    ],
    namespace="airflow",
    name="collect_video_id",
    in_cluster=True,
    volumes=[volume],
    volume_mounts=[volume_mount],
    image_pull_policy="Always",
    is_delete_operator_pod=True,
    do_xcom_push=True,
    dag=dag,
)

pub_exist_video_id = KubernetesPodOperator(
    task_id="pub_exist_video_id", 
    image="geup/pub_exist_video:test",
    #cmds=[""],
    arguments=[
        "--schema", "{{ conn.rabbitmq_video_id.schema }}",
        "--host", "{{ conn.rabbitmq_video_id.host }}",
        "--port", "{{ conn.rabbitmq_video_id.port }}",
        "--login", "{{ conn.rabbitmq_video_id.login }}",
        "--password", "{{ conn.rabbitmq_video_id.password }}",
        "--vhost", "{{ (conn.rabbitmq_video_id.extra | from_str_to_json)['vhost'] }}",
        "--routing_key", "exist_video_q",
        "--exist_video_id_list", "{{ task_instance.xcom_pull(task_ids='collect_video_id', key='return_value')['exist_video_id_list']}}"
    ],
    namespace="airflow",
    name="pub_exist_video_id",
    in_cluster=True,
    image_pull_policy="IfNotPresent",
    is_delete_operator_pod=True,
    dag=dag,
)

pub_not_exist_video_id = KubernetesPodOperator(
    task_id="pub_not_exist_video_id", 
    image="geup/pub_not_exist_video:test",
    #cmds=[""],
    arguments=[
        "--schema", "{{ conn.rabbitmq_video_id.schema }}",
        "--host", "{{ conn.rabbitmq_video_id.host }}",
        "--port", "{{ conn.rabbitmq_video_id.port }}",
        "--login", "{{ conn.rabbitmq_video_id.login }}",
        "--password", "{{ conn.rabbitmq_video_id.password }}",
        "--vhost", "{{ (conn.rabbitmq_video_id.extra | from_str_to_json)['vhost'] }}",
        "--routing_key", "not_exist_video_q",
        "--not_exist_video_id_list", "{{ (task_instance.xcom_pull(task_ids='collect_video_id', key='return_value') | from_str_to_json)['not_exist_video_id_list']}}"
    ],
    namespace="airflow",
    name="pub_not_exist_video_id",
    in_cluster=True,
    image_pull_policy="IfNotPresent",
    is_delete_operator_pod=True,
    dag=dag,
)

collect_video_id >> pub_exist_video_id
collect_video_id >> pub_not_exist_video_id

# collect_not_exist_video_comment = DockerOperator(
#     task_id="collect_not_exist_video_comment", 
#     image="geup/collect_not_exist_video_comment:test",
#     command=[
#         "--output_path", output_path,
#         "--token", "{{ conn.youtube_data_api.password }}",
#         "--schema", "{{ conn.rabbitmq_video_id.schema }}",
#         "--host", "{{ conn.rabbitmq_video_id.host }}",
#         "--port", "{{ conn.rabbitmq_video_id.port }}",
#         "--password", "{{ conn.rabbitmq_video_id.schema }}",
#         "--extra", "{{ conn.rabbitmq_video_id.extra }}",
#         "--routing_key", "not_exist_video_q",
#     ],
#     api_version="auto",
#     auto_remove=True,
#     mounts=[Mount(source="/root/comments", target="/comments", type="bind")],
#     docker_url = "unix://var/run/docker.sock",
#     network_mode="host",
#     mount_tmp_dir=False,
#     dag=dag,
# )

# collect_exist_video_comment = DockerOperator(
#     task_id="collect_exist_video_comment", 
#     image="geup/collect_exist_video_comment:test",
#     command=[
#         "--output_path", output_path,
#         "--token", "{{ conn.youtube_data_api.password }}",
#         "--schema", "{{ conn.rabbitmq_video_id.schema }}",
#         "--host", "{{ conn.rabbitmq_video_id.host }}",
#         "--port", "{{ conn.rabbitmq_video_id.port }}",
#         "--password", "{{ conn.rabbitmq_video_id.schema }}",
#         "--extra", "{{ conn.rabbitmq_video_id.extra }}",
#         "--routing_key", "exist_video_q",
#     ],
#     api_version="auto",
#     auto_remove=True,
#     mounts=[Mount(source="/root/comments", target="/comments", type="bind")],
#     docker_url = "unix://var/run/docker.sock",
#     network_mode="host",
#     mount_tmp_dir=False,
#     dag=dag,
# )


# pub_not_exist_video_id >> collect_not_exist_video_comment
# pub_exist_video_id >> collect_exist_video_comment
