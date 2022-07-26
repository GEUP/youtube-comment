#!/usr/bin/env python
from googleapiclient.discovery import build
import logging
import click
import os
import json
import rabbitpy

logging.basicConfig(level=logging.INFO)

new_comment_cnt = 0
updated_comment_text_cnt = 0
updated_comment_like_cnt = 0
new_reply_cnt = 0
updated_reply_text_cnt = 0
updated_reply_like_cnt = 0

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

def request_comment_response(api_obj, id, part, fields):
    response = (
        api_obj.comments()
        .list(
            part=part,
            id=id,
            fields=fields,
        )
        .execute()
    )
    return response

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

def modify_replies(api_obj, parentId, comments_dict):
    
    global new_reply_cnt, updated_reply_text_cnt, updated_reply_like_cnt

    items = get_all_replies_list(
        api_obj=api_obj,
        parentId=parentId,
        part="id,snippet",
        fields="items(id,snippet(updatedAt,likeCount)), nextPageToken",
    )
    for item in items:
        id = item["id"]
        updatedAt = item["snippet"]["updatedAt"]
        likeCount = item["snippet"]["likeCount"]

        if id not in comments_dict[parentId]["replies"]:
            new_reply_cnt += 1

            tmp_response = request_comment_response(
                api_obj=api_obj,
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
                updated_reply_text_cnt += 1
                tmp_response = request_comment_response(
                    api_obj=api_obj,
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
                updated_reply_like_cnt += 1
                comments_dict[parentId]["replies"][id]["likeCount"] = likeCount

def modify_comment_json(api_obj, video_id, comments_dict):

    global new_comment_cnt, updated_comment_text_cnt, updated_comment_like_cnt, new_reply_cnt, updated_reply_text_cnt, updated_reply_like_cnt

    part = "snippet,id"
    fields = "items(snippet(totalReplyCount,topLevelComment(id,snippet(updatedAt,likeCount)))), nextPageToken"

    response = request_comment_threads_response(
        api_obj=api_obj,
        video_id=video_id,
        part=part,
        fields=fields,
    )
    total_response_page=1
    while response:
        
        for item in response["items"]:
            id = item["snippet"]["topLevelComment"]["id"]
            totalReplyCount = item["snippet"]["totalReplyCount"]
            updatedAt = item["snippet"]["topLevelComment"]["snippet"]["updatedAt"]
            likeCount = item["snippet"]["topLevelComment"]["snippet"]["likeCount"]
            if id not in comments_dict:
                new_comment_cnt += 1
                tmp_response = request_comment_response(
                    api_obj=api_obj,
                    id=id,
                    part="snippet",
                    fields="items(snippet(textOriginal,authorDisplayName))",
                )
                tmp_item = tmp_response["items"][0]
                textOriginal = tmp_item["snippet"]["textOriginal"]
                authorDisplayName = tmp_item["snippet"]["authorDisplayName"]
                append_new_comment(
                    api_obj=api_obj,
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
                modify_replies(api_obj=api_obj, parentId=id, comments_dict=comments_dict)

                if comments_dict[id]["updatedAt"] != updatedAt:
                    updated_comment_text_cnt += 1
                    tmp_response = request_comment_response(
                        api_obj=api_obj,
                        id=id,
                        part="snippet",
                        fields="items(snippet(textOriginal))",
                    )
                    tmp_item = tmp_response["items"][0]
                    textOriginal = tmp_item["snippet"]["textOriginal"]
                    comments_dict[id]["updatedAt"] = updatedAt
                    comments_dict[id]["textOriginal"] = textOriginal

                if comments_dict[id]["likeCount"] != likeCount:
                    updated_comment_like_cnt += 1
                    comments_dict[id]["likeCount"] = likeCount

        if "nextPageToken" in response:
            total_response_page += 1
            response = request_next_comment_threads_response(
                api_obj=api_obj,
                video_id=video_id,
                next_page_token=response["nextPageToken"],
                part=part,
                fields=fields,
            )
        else:
            break
        
    print("total_response_page : ", total_response_page)
    print("new_comment_cnt : ", new_comment_cnt)
    print("updated_comment_text_cnt : ", updated_comment_text_cnt)
    print("updated_comment_like_cnt : ", updated_comment_like_cnt)
    print("new_reply_cnt : ", new_reply_cnt)
    print("updated_reply_text_cnt : ", updated_reply_text_cnt)
    print("updated_reply_like_cnt : ", updated_reply_like_cnt)

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
        print(video_id)

        with open(
            f"{output_path}/result_json_{video_id}", "r", encoding="utf-8"
        ) as rf:
            comments_json = rf.read()

        comments_dict = json.loads(comments_json)
        print("video id : ", video_id)
        modify_comment_json(api_obj, video_id, comments_dict)
        result_json = json.dumps(comments_dict)

        with open(
            f"{output_path}/result_json_{video_id}",
            mode="w",
            encoding="utf-8",
        ) as f:
            f.write(result_json)

        msg.ack()
    
    
if __name__=="__main__":
    main()