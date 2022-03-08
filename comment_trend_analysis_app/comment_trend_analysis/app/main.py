from fastapi import FastAPI, Form, Request, UploadFile, File
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from starlette.responses import StreamingResponse

from typing import List

import datetime

from db import DataBase

from io import BytesIO


app = FastAPI()
app.mount("/statics", StaticFiles(directory="./statics"), name="static")
templates = Jinja2Templates(directory="./templates")

database = DataBase()


def trend_analysis(query_date: str, max_token_size=150):

    query_date = datetime.datetime.strptime(query_date, "%Y-%m-%d").date()
    min_six_day = query_date - datetime.timedelta(days=6)
    max_six_day = query_date - datetime.timedelta(days=1)

    past_min_six_day = query_date - datetime.timedelta(days=13)
    past_max_six_day = query_date - datetime.timedelta(days=8)

    commnet_cnt_dict = database.select_commnet_cnt(past_min_six_day, query_date)
    query_date_cnt = commnet_cnt_dict[query_date]
    max_six_day_cnt = commnet_cnt_dict[max_six_day]

    six_day_cnt = database.commnet_cnt_between(
        commnet_cnt_dict, min_six_day, max_six_day
    )
    past_six_day_cnt = database.commnet_cnt_between(
        commnet_cnt_dict, past_min_six_day, past_max_six_day
    )

    trend_tokens, _ = database.select_trend_tokens(query_date)
    _, similar_tokens_df = database.select_similar_tokens(query_date)
    tsne, _ = database.select_tsne(query_date)

    sorted_token_cnt = []
    similar_token_dict = {}
    similar_token_score_dict = {}
    for tt in trend_tokens[:max_token_size]:
        sorted_token_cnt.append((tt["token"], tt["score"]))
        similar_token_dict[tt["token"]] = similar_tokens_df.loc[tt["token"]][
            "similar_token_list"
        ][1:-1].split(",")
        similar_token_score_dict[tt["token"]] = [
            round(float(x), 2)
            for x in similar_tokens_df.loc[tt["token"]]["score_list"][1:-1].split(",")
        ]

    tsne_list = []
    for tt in tsne:
        if tt["token"] in similar_token_dict:
            tsne_list.append([tt["token"], tt["x"], tt["y"]])
    tsne_list = sorted(tsne_list, key=lambda x: x[1])

    return {
        "query_date": query_date.strftime("%Y-%m-%d"),
        "week_up_down_percent": (
            ((six_day_cnt - past_six_day_cnt) / past_six_day_cnt) * 100
            if past_six_day_cnt != 0
            else 0
        ),
        "day_up_down_percent": (
            ((query_date_cnt - max_six_day_cnt) / max_six_day_cnt) * 100
            if max_six_day_cnt != 0
            else 0
        ),
        "week_comment_cnt": six_day_cnt,
        "day_comment_cnt": query_date_cnt,
        "sorted_token_cnt": sorted_token_cnt,
        "similar_token_dict": similar_token_dict,
        "similar_token_score_dict": similar_token_score_dict,
        "tsne_list": tsne_list,
    }


@app.get("/")
def index(request: Request):
    trendAt_list = database.select_trendAt()
    min_trendAt = min(trendAt_list)
    max_trendAt = max(trendAt_list)

    return templates.TemplateResponse(
        "index.html",
        context={
            "request": request,
            "min_trendAt": min_trendAt,
            "max_trendAt": max_trendAt,
        },
    )


@app.get("/comment")
def comment(query_date: str, max_token_size: int):
    return trend_analysis(query_date, max_token_size)
