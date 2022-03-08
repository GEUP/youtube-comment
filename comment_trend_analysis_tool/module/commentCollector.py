from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import cryptocode
import os


class commentCollector:
    def __init__(self):

        with open("password.txt", "r") as f:
            password = cryptocode.decrypt(f.read(), os.environ["master_key"]).split(
                "\n"
            )
        api_key = password[0].strip()

        self.api_obj = build("youtube", "v3", developerKey=api_key)
        self.maxResults = 50

    def request_mostPopular_videos(self):
        most_popular_video_id_and_title_list = list()

        response = (
            self.api_obj.videos()
            .list(
                part="id,snippet",
                regionCode="KR",
                chart="mostPopular",
                maxResults=self.maxResults,
                fields="items(id, snippet(title)), nextPageToken",
            )
            .execute()
        )

        while response:
            for i, item in enumerate(response["items"]):
                id = item["id"]
                title = item["snippet"]["title"]
                most_popular_video_id_and_title_list.append((id, title))

            if "nextPageToken" in response:
                response = (
                    self.api_obj.videos()
                    .list(
                        part="id,snippet",
                        regionCode="KR",
                        chart="mostPopular",
                        maxResults=self.maxResults,
                        fields="items(id, snippet(title)), nextPageToken",
                        pageToken=response["nextPageToken"],
                    )
                    .execute()
                )
            else:
                break

        return most_popular_video_id_and_title_list

    def request_comment(self, video_id):
        response = (
            self.api_obj.commentThreads()
            .list(
                part="snippet,replies",
                videoId=video_id,
                maxResults=self.maxResults,
                order="time",
                fields="items(snippet(totalReplyCount,topLevelComment(snippet(textOriginal,authorDisplayName,publishedAt,likeCount))),replies(comments(snippet(textOriginal, authorDisplayName, publishedAt, likeCount)))), nextPageToken",
            )
            .execute()
        )

        return response

    def request_next_comment(self, video_id, old_response):
        response = (
            self.api_obj.commentThreads()
            .list(
                part="snippet,replies",
                videoId=video_id,
                pageToken=old_response["nextPageToken"],
                maxResults=self.maxResults,
            )
            .execute()
        )

        return response
