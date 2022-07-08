from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from custom_component.hooks import RabbitMQHook, YoutubeDataAPIHook
from airflow.utils.log.logging_mixin import LoggingMixin
import os
import json


class RabbitMQOperator(BaseOperator):
    templeate_fields = {"_start_date"}

    @apply_defaults
    def __init__(self, conn_id, xcom_key, routing_key, start_date="{{ds}}", **kwargs):
        super().__init__(**kwargs)
        self._start_date = start_date
        self._conn_id = conn_id
        self._xcom_key = xcom_key
        self._routing_key = routing_key
        self._kwargs = kwargs

    def execute(self, context):
        LoggingMixin().log.info(
            "kwargs?" + str(self._kwargs) + "start_date?" + str(self._start_date)
        )
        video_id_list = context["task_instance"].xcom_pull(
            task_ids="collect_video_id", key=self._xcom_key
        )
        hook = RabbitMQHook(conn_id=self._conn_id)
        hook.pub_video_id_list(self._routing_key, video_id_list[:3])  # for test


class CollectYoutubeVideoIDOperator(BaseOperator):
    # template_fields =
    @apply_defaults
    def __init__(self, output_path, conn_id, **kwargs):
        super().__init__(**kwargs)
        self._output_path = output_path
        self._conn_id = conn_id

    def execute(self, context):
        hook = YoutubeDataAPIHook(self._conn_id)
        most_popular_video_id_and_title_list = list(
            hook.get_most_popular_video_id_and_title_generator()
        )
        exist_video_id_list = []
        not_exist_video_id_list = []
        for video_id, _ in most_popular_video_id_and_title_list:
            if os.path.exists(f"{self._output_path}/result_json_{video_id}"):
                exist_video_id_list.append(video_id)
            else:
                not_exist_video_id_list.append(video_id)
        context["task_instance"].xcom_push(
            key="exist_video_id_list", value=exist_video_id_list
        )
        context["task_instance"].xcom_push(
            key="not_exist_video_id_list", value=not_exist_video_id_list
        )


class CollectNotExistYoutubeVideoCommentsOperator(BaseOperator):
    # template_fields =
    @apply_defaults
    def __init__(
        self,
        output_path,
        youtube_data_api_conn_id,
        rabbitmq_conn_id,
        queue_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._output_path = output_path
        self._youtube_data_api_conn_id = youtube_data_api_conn_id
        self._rabbitmq_conn_id = rabbitmq_conn_id
        self._queue_name = queue_name

    def execute(self, context):
        youtube_hook = YoutubeDataAPIHook(conn_id=self._youtube_data_api_conn_id)
        rabbitmq_hook = RabbitMQHook(conn_id=self._rabbitmq_conn_id)

        queue = rabbitmq_hook.get_queue(queue_name=self._queue_name)

        while len(queue) > 0:
            msg = queue.get()
            video_id = msg.body.decode("utf-8")
            LoggingMixin().log.info("video id : " + str(video_id))
            comment_json = youtube_hook.get_comment_json(video_id)

            with open(
                f"{self._output_path}/result_json_{video_id}",
                mode="w",
                encoding="utf-8",
            ) as f:
                f.write(comment_json)

            msg.ack()


class CollectExistYoutubeVideoCommentsOperator(BaseOperator):
    # template_fields =
    @apply_defaults
    def __init__(
        self,
        output_path,
        youtube_data_api_conn_id,
        rabbitmq_conn_id,
        queue_name,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._output_path = output_path
        self._youtube_data_api_conn_id = youtube_data_api_conn_id
        self._rabbitmq_conn_id = rabbitmq_conn_id
        self._queue_name = queue_name

    def execute(self, context):
        youtube_hook = YoutubeDataAPIHook(conn_id=self._youtube_data_api_conn_id)
        rabbitmq_hook = RabbitMQHook(conn_id=self._rabbitmq_conn_id)

        queue = rabbitmq_hook.get_queue(queue_name=self._queue_name)

        while len(queue) > 0:
            msg = queue.get()
            video_id = msg.body.decode("utf-8")
            LoggingMixin().log.info(video_id)

            with open(
                f"{self._output_path}/result_json_{video_id}", "r", encoding="utf-8"
            ) as rf:
                comments_json = rf.read()

            comments_dict = json.loads(comments_json)
            LoggingMixin().log.info("video id : " + str(video_id))
            youtube_hook.modify_comment_json(video_id, comments_dict)
            result_json = json.dumps(comments_dict)

            with open(
                f"{self._output_path}/result_json_{video_id}",
                mode="w",
                encoding="utf-8",
            ) as f:
                f.write(result_json)

            msg.ack()
