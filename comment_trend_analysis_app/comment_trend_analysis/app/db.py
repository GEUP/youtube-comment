import pandas as pd
import os
import cryptocode
from tqdm import tqdm
from impala.dbapi import connect


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
    def __init__(self):

        with open("./password.txt", "r") as f:
            password = cryptocode.decrypt(f.read(), os.environ["master_key"]).split(
                "\n"
            )
            
        db_ip = password[1].strip()

        self.db = connect(host=db_ip, port=10000, user='hadoop_user', auth_mechanism='PLAIN')

        self.cursor = self.db.cursor()
        self.cursor.execute("use comment")

    def select_commnet_cnt(self, mindate: datetime.date, maxdate: datetime.date):
        sql = "SELECT DATE(CreateAt), COUNT(*) FROM comment_table WHERE DATE(CreateAt) BETWEEN '{}' AND '{}' group by DATE(CreateAt) ".format(
            mindate.strftime("%Y-%m-%d"), maxdate.strftime("%Y-%m-%d")
        )
        self.cursor.execute(sql)
        cnt_per_date = self.cursor.fetchall()
        
        cnt_per_date_dict_list = []
        for result in cnt_per_date:            
            cnt_per_date_dict_list.append({'DATE(CreateAt)':result[0],'COUNT(*)':result[1]})

        comment_cnt_dict = {}
        for comment_cnt in cnt_per_date_dict_list:
            comment_cnt_dict[comment_cnt["DATE(CreateAt)"]] = comment_cnt["COUNT(*)"]

        return comment_cnt_dict

    def commnet_cnt_between(
        self, comment_cnt_dict: dict, mindate: datetime.date, maxdate: datetime.date
    ):
        cnt = 0
        for date, value in comment_cnt_dict.items():
            if (date >= mindate) and (date <= maxdate):
                cnt += value
        return cnt

    def select_trend_tokens(self, date: datetime.date):
        sql = (
            "SELECT token, score FROM trend_token_table  WHERE trendAt = '{}' ".format(
                date.strftime("%Y-%m-%d")
            )
        )
        self.cursor.execute(sql)
        trend_tokens = self.cursor.fetchall()
        trend_tokens_dict_list = []
        for result in trend_tokens:            
            trend_tokens_dict_list.append({'token':result[0],'score':result[1]})
            
        trend_tokens_df = pd.DataFrame(trend_tokens_dict_list)
        return trend_tokens_dict_list, trend_tokens_df

    def select_similar_tokens(self, date: datetime.date):
        sql = "SELECT token, similar_token_list, score_list FROM similar_token_table  WHERE trendAt = '{}' ".format(
            date.strftime("%Y-%m-%d")
        )
        self.cursor.execute(sql)
        similar_tokens = self.cursor.fetchall()
        similar_tokens_dict_list = []
        for result in similar_tokens:            
            similar_tokens_dict_list.append({'token':result[0],'similar_token_list':result[1], 'score_list':result[2]})
        similar_tokens_df = pd.DataFrame(similar_tokens_dict_list)
        similar_tokens_df = similar_tokens_df.set_index("token")
        return similar_tokens_dict_list, similar_tokens_df

    def select_tsne(self, date: datetime.date):
        sql = "SELECT token, x, y FROM tsne_table WHERE trendAt = '{}' ".format(
            date.strftime("%Y-%m-%d")
        )
        self.cursor.execute(sql)
        tsne = self.cursor.fetchall()
        similar_tokens_dict_list = []
        for result in tsne:            
            similar_tokens_dict_list.append({'token':result[0],'x':result[1], 'y':result[2]})
        tsne_df = pd.DataFrame(similar_tokens_dict_list)
        tsne_df = tsne_df.set_index("token")
        return similar_tokens_dict_list, tsne_df

    def select_trendAt(self):
        sql = "SELECT DATE(trendAt) FROM trend_token_table group by DATE(trendAt)"
        self.cursor.execute(sql)
        trendAt_list = self.cursor.fetchall()
        trendAt_list_dict_list = []
        for result in trendAt_list:            
            trendAt_list_dict_list.append({'DATE(trendAt)':result[0]})
        date_list = []
        for trendAt in trendAt_list_dict_list:
            date_list.append(trendAt["DATE(trendAt)"])
        return date_list

    # def show_isolation_level(self):
    #     sql = "SHOW VARIABLES LIKE '%isolation'"
    #     self.cursor.execute(sql)
    #     isolation_level = self.cursor.fetchall()
    #     print(isolation_level)
