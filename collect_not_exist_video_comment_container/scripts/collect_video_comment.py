#!/usr/bin/env python
from googleapiclient.discovery import build
import logging
import click
import os
import json
import rabbitpy

logging.basicConfig(level=logging.INFO)

def request_comment_threads_response(api_obj, video_id, part, fields, maxResults=100):
    response = (
        api_obj.commentThreads()
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

def request_next_comment_threads_response(
    api_obj, video_id, next_page_token, part, fields, maxResults=100
):
    response = (
        api_obj.commentThreads()
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

def request_replies_response(api_obj, parentId, part, fields, maxResults=100):
    response = (
        api_obj.comments()
        .list(
            part=part,
            parentId=parentId,
            maxResults=maxResults,
            fields=fields,
        )
        .execute()
    )

    return response

def request_next_replies_response(
    api_obj, parentId, next_page_token, part, fields, maxResults=100
):
    response = (
        api_obj.comments()
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

def get_all_replies_list(
    api_obj,
    parentId,
    part="snippet",
    fields="items(id,snippet(textOriginal,authorDisplayName,updatedAt,likeCount)), nextPageToken",
):
    response = request_replies_response(
        api_obj=api_obj,
        parentId=parentId,
        part=part,
        fields=fields,
    )
    items = response["items"]
    while response:
        if "nextPageToken" in response:
            response = request_next_replies_response(
                api_obj=api_obj,
                parentId=parentId,
                next_page_token=response["nextPageToken"],
                part=part,
                fields=fields,
            )
            items.extend(response["items"])
        else:
            break
    return items

def append_new_comment(
    api_obj,
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
            for reply in get_all_replies_list(api_obj, id)
        }

def get_comment_json(api_obj, video_id):
    
    response = request_comment_threads_response(
        api_obj=api_obj,
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
            append_new_comment(
                api_obj=api_obj,
                result_dict=result,
                id=id,
                updatedAt=updatedAt,
                totalReplyCount=totalReplyCount,
                textOriginal=textOriginal,
                authorDisplayName=authorDisplayName,
                likeCount=likeCount,
            )
        if "nextPageToken" in response:
            response = request_next_comment_threads_response(
                api_obj=api_obj,
                video_id=video_id,
                next_page_token=response["nextPageToken"],
                part="snippet,id",
                fields="items(snippet(totalReplyCount,topLevelComment(id,snippet(textOriginal,authorDisplayName,updatedAt,likeCount)))), nextPageToken",
            )

        else:
            break
    return json.dumps(result)

def get_queue(schema, host, port, login, password, vhost, routing_key):
    
    base_url = f"{schema}://{login}:{password}@{host}:{port}/{vhost}"
    conn = rabbitpy.Connection(base_url)
    chan = conn.channel()
    tx = rabbitpy.Tx(chan)

    queue = rabbitpy.Queue(chan, routing_key)

    return queue

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
@click.option(
    "--schema",
    type=str,
    required=True,
    help="RabbitMQ schema"
)
@click.option( 
    "--host",
    type=str,
    required=True,
    help="RabbitMQ host"
)
@click.option(
    "--port",
    type=str,
    required=True,
    help="RabbitMQ port"
)
@click.option(
    "--login",
    type=str,
    required=True,
    help="RabbitMQ login"
)    
@click.option( 
    "--password",
    type=str,
    required=True,
    help="RabbitMQ password"
)  
@click.option(
    "--vhost",
    type=str,
    required=True,
    help="RabbitMQ vhost"
)  
@click.option(
    "--routing_key",
    type=str,
    required=True,
    help="RabbitMQ queue routing_key"
)   
def main(output_path, token, schema, host, port, login, password, vhost, routing_key):
    api_obj = build("youtube", "v3", developerKey=token)
    queue = get_queue(schema, host, port, login, password, vhost, routing_key)

    while len(queue) > 0:
        msg = queue.get()
        video_id = msg.body.decode("utf-8")

        comment_json = get_comment_json(api_obj, video_id)

        with open(
            f"{output_path}/result_json_{video_id}",
            mode="w",
            encoding="utf-8",
        ) as f:
            f.write(comment_json)

        msg.ack()
    
    
if __name__=="__main__":
    main()