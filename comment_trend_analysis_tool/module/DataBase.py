import pandas as pd
import os
import cryptocode
from tqdm import tqdm
from impala.dbapi import connect

from module.utils import clean_text

import datetime

'''
CREATE TABLE comment_table(
    > video_id STRING,
    > top_id INT,
    > bottom_id INT,
    > comment STRING,
    > author STRING,
    > createAt TIMESTAMP,
    > num_likes INT);

CREATE TABLE similar_token_table(
    > token STRING,
    > similar_token_list STRING,
    > score_list STRING,
    > trendAt TIMESTAMP);


CREATE TABLE trend_token_table(
    > token STRING,
    > score FLOAT,
    > trendAt TIMESTAMP);


CREATE TABLE tsne_table(
    > token STRING,
    > x FLOAT,
    > y FLOAT,
    > trendAt TIMESTAMP);

'''
class DataBase:
    def __init__(self, week):

        self.week = week

        with open("./password.txt", "r") as f:
            password = cryptocode.decrypt(f.read(), os.environ["master_key"]).split(
                "\n"
            )
            
        db_ip = password[1].strip()

        self.db = connect(host=db_ip, port=10000, user='hadoop_user', auth_mechanism='PLAIN')

        self.cursor = self.db.cursor()
        self.cursor.execute("use comment")

    def set_week(self, week):
        self.week = week

    def select_six_day_comment(self):
        sql = "SELECT comment FROM comment_table WHERE DATE(CreateAt) BETWEEN '{}' AND '{}' ".format(
            self.week[0], self.week[-2]
        )
        self.cursor.execute(sql)
        six_day_result = self.cursor.fetchall()

        six_day_result_dict_list = []
        for result in six_day_result:            
            six_day_result_dict_list.append({'comment':result[0]})

        six_day_result_df = pd.DataFrame(six_day_result_dict_list)

        return six_day_result_dict_list, six_day_result_df

    def select_queryday_comment(self):
        sql = "SELECT comment FROM comment_table WHERE DATE(CreateAt) = '{}' ".format(
            self.week[-1]
        )
        self.cursor.execute(sql)
        queryday_result = self.cursor.fetchall()\
        
        queryday_result_dict_list = []
        for result in queryday_result:
            queryday_result_dict_list.append({'comment':result[0]})

        queryday_result_df = pd.DataFrame(queryday_result_dict_list)

        return queryday_result_dict_list, queryday_result_df

    def insert_trend_token(self, sorted_queryday_token_cnt_df):
        sql = "INSERT INTO trend_token_table(token, score, trendAt) VALUES "
        for di, row in enumerate(
            tqdm(
                sorted_queryday_token_cnt_df.iterrows(),
                total=len(sorted_queryday_token_cnt_df),
            )
        ):
            sql += "('{}',{},'{}'),".format(
                row[1]["token"], row[1]["score"], self.week[-1]
            )
            if (di % 5000) == 0:
                self.cursor.execute(sql[:-1])
                print("inserted ", di)
                sql = "INSERT INTO trend_token_table(token, score, trendAt) VALUES "
    
        if sql != "INSERT INTO trend_token_table(token, score, trendAt) VALUES ":
            self.cursor.execute(sql[:-1])

    def insert_similar_token(self, similar_tokens_df):

        sql = "INSERT INTO similar_token_table(token, similar_token_list, score_list, trendAt) VALUES "
        for di, row in enumerate(
            tqdm(similar_tokens_df.iterrows(), total=len(similar_tokens_df))
        ):
            sql += "('{}','[{}]','[{}]','{}'),".format(
                row[0],
                ", ".join(row[1]["similar_token_list"]),
                ", ".join([str(score) for score in row[1]["score_list"]]),
                self.week[-1],
            )
            if (di % 5000) == 0:
                self.cursor.execute(sql[:-1])
                print("inserted ",di)
                sql = "INSERT INTO similar_token_table(token, similar_token_list, score_list, trendAt) VALUES "
        
        if sql != "INSERT INTO similar_token_table(token, similar_token_list, score_list, trendAt) VALUES ":
            self.cursor.execute(sql[:-1])

    def insert_tsne(self, tsne_df):
        sql = "INSERT INTO tsne_table(token, x, y, trendAt) VALUES "
        for di, row in enumerate(tqdm(tsne_df.iterrows(), total=len(tsne_df))):
            sql += "('{}',{},{},'{}'),".format(
                row[0], row[1]["x"], row[1]["y"], self.week[-1]
            )
            if (di % 5000) == 0:
                self.cursor.execute(sql[:-1])
                print("inserted ",di)
                sql = "INSERT INTO tsne_table(token, x, y, trendAt) VALUES "
        
        if sql != "INSERT INTO tsne_table(token, x, y, trendAt) VALUES ":
            self.cursor.execute(sql[:-1])

    def select_exist_video_and_max_date(self):
        sql = "SELECT video_id, DATE(MAX(DATE(CreateAt))), MAX(top_id) FROM comment_table GROUP BY video_id"
        self.cursor.execute(sql)
        select_result = self.cursor.fetchall()
        
        select_result_dict_list = []
        for result in select_result:
            select_result_dict_list.append({'video_id':result[0],'DATE(MAX(DATE(CreateAt)))':result[1],'MAX(top_id)':result[2]})
            
            
        return select_result_dict_list

    def insert_comment(self, checkpoint_manager, comment_df):
        publish_at_cnt = {}
        sql = "INSERT INTO comment_table(video_id, top_id, bottom_id, comment, author, createAt, num_likes) VALUES "
        for di, x in enumerate(tqdm(comment_df.iterrows(), total=len(comment_df))):
            createAt = x[1][5].replace("T", " ").replace("Z", "")
            sql += "('{}',{},{},'{}','{}','{}',{}),".format(
                x[1][0],
                x[1][1],
                x[1][2],
                x[1][3],
                clean_text(x[1][4]),
                createAt,
                x[1][6],
            )
            
            if (di % 5000) == 0:
                self.cursor.execute(sql[:-1])
                print("inserted ",di)
                sql = "INSERT INTO comment_table(video_id, top_id, bottom_id, comment, author, createAt, num_likes) VALUES "
            
            createAt = datetime.datetime.strptime(
                createAt.split(" ")[0], "%Y-%m-%d"
            ).date()

            if createAt in publish_at_cnt:
                publish_at_cnt[createAt] += 1
            else:
                publish_at_cnt[createAt] = 1
        if sql != "INSERT INTO comment_table(video_id, top_id, bottom_id, comment, author, createAt, num_likes) VALUES ":
            self.cursor.execute(sql[:-1])
        return publish_at_cnt

    def close(self):
        self.db.close()
