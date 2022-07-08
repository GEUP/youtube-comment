from airflow.hooks.base_hook import BaseHook
from airflow.utils.log.logging_mixin import LoggingMixin
import rabbitpy
import json
from googleapiclient.discovery import build


class RabbitMQHook(BaseHook):
    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._tx = None

    def get_tx(self):
        if self._tx == None:
            config = self.get_connection(self._conn_id)
            self.schema = config.schema
            self.host = config.host
            self.port = config.port
            base_url = f"{self.schema}://{config.login}:{config.password}@{self.host}:{self.port}/{json.loads(config.extra)['vhost']}"
            self._conn = rabbitpy.Connection(base_url)
            self._chan = self._conn.channel()
            self._tx = rabbitpy.Tx(self._chan)

        return self._tx

    def pub_video_id_list(self, routing_key, video_id_list):
        self.get_tx()
        self._tx.select()

        for video_id in video_id_list:
            msg = rabbitpy.Message(self._chan, video_id)
            msg.publish("", routing_key)

        self._tx.commit()

    def get_queue(self, queue_name):
        self.get_tx()
        queue = rabbitpy.Queue(self._chan, queue_name)
        return queue


class YoutubeDataAPIHook(BaseHook):
    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._api_obj = None

    def get_api_obj(self):

        if self._api_obj == None:
            config = self.get_connection(self._conn_id)
            token = config.password
            self._api_obj = build("youtube", "v3", developerKey=token)

        return self._api_obj

    def get_most_popular_video_id_and_title_generator(self, maxResults=50):
        self.get_api_obj()
        response = (
            self._api_obj.videos()
            .list(
                part="id,snippet",
                regionCode="KR",
                chart="mostPopular",
                maxResults=maxResults,
                fields="items(id, snippet(title)), nextPageToken",
            )
            .execute()
        )

        while response:
            for i, item in enumerate(response["items"]):
                id = item["id"]
                title = item["snippet"]["title"]
                yield (id, title)

            if "nextPageToken" in response:
                response = (
                    self._api_obj.videos()
                    .list(
                        part="id,snippet",
                        regionCode="KR",
                        chart="mostPopular",
                        maxResults=maxResults,
                        fields="items(id, snippet(title)), nextPageToken",
                        pageToken=response["nextPageToken"],
                    )
                    .execute()
                )
            else:
                break

    def _request_comment_threads_response(self, video_id, part, fields, maxResults=100):
        response = (
            self._api_obj.commentThreads()
            .list(
                part=part,
                videoId=video_id,
                maxResults=maxResults,
                order="time",
                fields=fields,
            )
            .execute()
        )

        return response

    def _request_next_comment_threads_response(
        self, video_id, next_page_token, part, fields, maxResults=100
    ):
        response = (
            self._api_obj.commentThreads()
            .list(
                part=part,
                videoId=video_id,
                pageToken=next_page_token,
                maxResults=maxResults,
                order="time",
                fields=fields,
            )
            .execute()
        )

        return response

    def _request_replies_response(self, parentId, part, fields, maxResults=100):
        response = (
            self._api_obj.comments()
            .list(
                part=part,
                parentId=parentId,
                maxResults=maxResults,
                fields=fields,
            )
            .execute()
        )

        return response

    def _request_next_replies_response(
        self, parentId, next_page_token, part, fields, maxResults=100
    ):
        response = (
            self._api_obj.comments()
            .list(
                part=part,
                parentId=parentId,
                pageToken=next_page_token,
                maxResults=maxResults,
                fields=fields,
            )
            .execute()
        )

        return response

    def _get_all_replies_list(
        self,
        parentId,
        part="snippet",
        fields="items(id,snippet(textOriginal,authorDisplayName,updatedAt,likeCount)), nextPageToken",
    ):
        response = self._request_replies_response(
            parentId=parentId,
            part=part,
            fields=fields,
        )
        items = response["items"]
        while response:
            if "nextPageToken" in response:
                response = self._request_next_replies_response(
                    parentId=parentId,
                    next_page_token=response["nextPageToken"],
                    part=part,
                    fields=fields,
                )
                items.extend(response["items"])
            else:
                break
        return items

    def _append_new_comment(
        self,
        result_dict,
        id,
        updatedAt,
        totalReplyCount,
        textOriginal,
        authorDisplayName,
        likeCount,
    ):

        result_dict[id] = {
            "totalReplyCount": totalReplyCount,
            "updatedAt": updatedAt,
            "textOriginal": textOriginal,
            "authorDisplayName": authorDisplayName,
            "likeCount": likeCount,
            "replies": {},
        }
        if totalReplyCount > 0:
            result_dict[id]["replies"] = {
                reply["id"]: {
                    "textOriginal": reply["snippet"]["textOriginal"],
                    "authorDisplayName": reply["snippet"]["authorDisplayName"],
                    "updatedAt": reply["snippet"]["updatedAt"],
                    "likeCount": reply["snippet"]["likeCount"],
                }
                for reply in self._get_all_replies_list(id)
            }

    def get_comment_json(self, video_id):
        self.get_api_obj()
        response = self._request_comment_threads_response(
            video_id=video_id,
            part="snippet,id",
            fields="items(snippet(totalReplyCount,topLevelComment(id,snippet(textOriginal,authorDisplayName,updatedAt,likeCount)))), nextPageToken",
        )

        result = {}

        while response:
            for item in response["items"]:
                id = item["snippet"]["topLevelComment"]["id"]
                updatedAt = item["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
                totalReplyCount = item["snippet"]["totalReplyCount"]
                textOriginal = item["snippet"]["topLevelComment"]["snippet"][
                    "textOriginal"
                ]
                authorDisplayName = item["snippet"]["topLevelComment"]["snippet"][
                    "authorDisplayName"
                ]
                likeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
                self._append_new_comment(
                    result_dict=result,
                    id=id,
                    updatedAt=updatedAt,
                    totalReplyCount=totalReplyCount,
                    textOriginal=textOriginal,
                    authorDisplayName=authorDisplayName,
                    likeCount=likeCount,
                )
            if "nextPageToken" in response:
                response = self._request_next_comment_threads_response(
                    video_id=video_id,
                    next_page_token=response["nextPageToken"],
                    part="snippet,id",
                    fields="items(snippet(totalReplyCount,topLevelComment(id,snippet(textOriginal,authorDisplayName,updatedAt,likeCount)))), nextPageToken",
                )

            else:
                break
        return json.dumps(result)

    def _request_comment_response(self, id, part, fields):
        response = (
            self._api_obj.comments()
            .list(
                part=part,
                id=id,
                fields=fields,
            )
            .execute()
        )
        return response

    def _append_new_comment(
        self,
        result_dict,
        id,
        updatedAt,
        totalReplyCount,
        textOriginal,
        authorDisplayName,
        likeCount,
    ):

        result_dict[id] = {
            "totalReplyCount": totalReplyCount,
            "updatedAt": updatedAt,
            "textOriginal": textOriginal,
            "authorDisplayName": authorDisplayName,
            "likeCount": likeCount,
            "replies": {},
        }
        if totalReplyCount > 0:
            result_dict[id]["replies"] = {
                reply["id"]: {
                    "textOriginal": reply["snippet"]["textOriginal"],
                    "authorDisplayName": reply["snippet"]["authorDisplayName"],
                    "updatedAt": reply["snippet"]["updatedAt"],
                    "likeCount": reply["snippet"]["likeCount"],
                }
                for reply in self._get_all_replies_list(id)
            }

    def _modify_replies(self, parentId, comments_dict):
        self.get_api_obj()

        items = self._get_all_replies_list(
            parentId=parentId,
            part="id,snippet",
            fields="items(id,snippet(updatedAt,likeCount)), nextPageToken",
        )
        for item in items:
            id = item["id"]
            updatedAt = item["snippet"]["updatedAt"]
            likeCount = item["snippet"]["likeCount"]

            if id not in comments_dict[parentId]["replies"]:
                self.new_reply_cnt += 1

                tmp_response = self._request_comment_response(
                    id=id,
                    part="snippet",
                    fields="items(snippet(textOriginal,authorDisplayName))",
                )
                reply = tmp_response["items"][0]
                comments_dict[parentId]["replies"][id] = {
                    "textOriginal": reply["snippet"]["textOriginal"],
                    "authorDisplayName": reply["snippet"]["authorDisplayName"],
                    "updatedAt": updatedAt,
                    "likeCount": likeCount,
                }
            else:

                if comments_dict[parentId]["replies"][id]["updatedAt"] != updatedAt:
                    self.updated_reply_text_cnt += 1
                    tmp_response = self._request_comment_response(
                        id=id,
                        part="snippet",
                        fields="items(snippet(textOriginal))",
                    )
                    tmp_item = tmp_response["items"][0]
                    textOriginal = tmp_item["snippet"]["textOriginal"]
                    comments_dict[parentId]["replies"][id]["updatedAt"] = updatedAt
                    comments_dict[parentId]["replies"][id][
                        "textOriginal"
                    ] = textOriginal
                if comments_dict[parentId]["replies"][id]["likeCount"] != likeCount:
                    self.updated_reply_like_cnt += 1
                    comments_dict[parentId]["replies"][id]["likeCount"] = likeCount

    def modify_comment_json(self, video_id, comments_dict):

        self.get_api_obj()
        self.new_comment_cnt = 0
        self.updated_comment_text_cnt = 0
        self.updated_comment_like_cnt = 0
        self.new_reply_cnt = 0
        self.updated_reply_text_cnt = 0
        self.updated_reply_like_cnt = 0

        part = "snippet,id"
        fields = "items(snippet(totalReplyCount,topLevelComment(id,snippet(updatedAt,likeCount)))), nextPageToken"

        response = self._request_comment_threads_response(
            video_id=video_id,
            part=part,
            fields=fields,
        )

        while response:
            for item in response["items"]:
                id = item["snippet"]["topLevelComment"]["id"]
                totalReplyCount = item["snippet"]["totalReplyCount"]
                updatedAt = item["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
                likeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
                if id not in comments_dict:
                    self.new_comment_cnt += 1
                    tmp_response = self._request_comment_response(
                        id=id,
                        part="snippet",
                        fields="items(snippet(textOriginal,authorDisplayName))",
                    )
                    tmp_item = tmp_response["items"][0]
                    textOriginal = tmp_item["snippet"]["textOriginal"]
                    authorDisplayName = tmp_item["snippet"]["authorDisplayName"]
                    self._append_new_comment(
                        result_dict=comments_dict,
                        id=id,
                        updatedAt=updatedAt,
                        totalReplyCount=totalReplyCount,
                        textOriginal=textOriginal,
                        authorDisplayName=authorDisplayName,
                        likeCount=likeCount,
                    )
                else:
                    comments_dict[id]["totalReplyCount"] = totalReplyCount
                    self._modify_replies(parentId=id, comments_dict=comments_dict)

                    if comments_dict[id]["updatedAt"] != updatedAt:
                        self.updated_comment_text_cnt += 1
                        tmp_response = self._request_comment_response(
                            id=id,
                            part="snippet",
                            fields="items(snippet(textOriginal))",
                        )
                        tmp_item = tmp_response["items"][0]
                        textOriginal = tmp_item["snippet"]["textOriginal"]
                        comments_dict[id]["updatedAt"] = updatedAt
                        comments_dict[id]["textOriginal"] = textOriginal

                    if comments_dict[id]["likeCount"] != likeCount:
                        self.updated_comment_like_cnt += 1
                        comments_dict[id]["likeCount"] = likeCount

            if "nextPageToken" in response:
                response = self._request_next_comment_threads_response(
                    video_id=video_id,
                    next_page_token=response["nextPageToken"],
                    part=part,
                    fields=fields,
                )

            else:
                break
        LoggingMixin().log.info("new_comment_cnt : " + str(self.new_comment_cnt))
        LoggingMixin().log.info(
            "updated_comment_text_cnt : " + str(self.updated_comment_text_cnt)
        )
        LoggingMixin().log.info(
            "updated_comment_like_cnt : " + str(self.updated_comment_like_cnt)
        )
        LoggingMixin().log.info("new_reply_cnt : " + str(self.new_reply_cnt))
        LoggingMixin().log.info(
            "updated_reply_text_cnt : " + str(self.updated_reply_text_cnt)
        )
        LoggingMixin().log.info(
            "updated_reply_like_cnt : " + str(self.updated_reply_like_cnt)
        )
