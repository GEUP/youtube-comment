#!/usr/bin/env python

from googleapiclient.discovery import build
import logging
import click
import os
import json


logging.basicConfig(level=logging.INFO)



def get_most_popular_video_id_and_title_generator(api_obj, maxResults=50):
        
        response = (
            api_obj.videos()
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
                    api_obj.videos()
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
            
@click.command()
@click.option(
    "--output_path",
    type=str,
    required=True,
    help="Output path for find exist comments"
)
@click.option(
    "--token",
    type=str,
    required=True,
    help="Youtube Data API Token"
)         
def main(output_path, token):
    api_obj = build("youtube", "v3", developerKey=token)
    
    most_popular_video_id_and_title_list = list(get_most_popular_video_id_and_title_generator(api_obj))
    
    exist_video_id_list = []
    not_exist_video_id_list = []
    for video_id, _ in most_popular_video_id_and_title_list:
        if os.path.exists(f"{output_path}/result_json_{video_id}"):
            exist_video_id_list.append(video_id)
        else:
            not_exist_video_id_list.append(video_id)
    
    if not os.path.exists("/airflow/xcom"):
        os.makedirs("/airflow/xcom")
    with open("/airflow/xcom/return.json", mode="w") as f:
        f.write(json.dumps({"exist_video_id_list": exist_video_id_list, "not_exist_video_id_list": not_exist_video_id_list}))
    
    
if __name__=="__main__":
    main()