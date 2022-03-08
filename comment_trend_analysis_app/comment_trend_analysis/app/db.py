import pymysql
import datetime
import pandas as pd
import cryptocode
import os


class DataBase:
    def __init__(self):

        with open("./app/password.txt", "r") as f:
            password = cryptocode.decrypt(f.read(), os.environ['master_key']).split('\n')
        db_pw = password[1].strip()
        db_ip = password[2].strip()

        self.db = pymysql.connect(
            user="root",
            passwd=db_pw,
            host=db_ip,
            db="comment",
            charset="utf8mb4",
            autocommit=False,
        )

        self.cursor = self.db.cursor(pymysql.cursors.DictCursor)
        self.cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED;")

    def select_commnet_cnt(self, mindate: datetime.date, maxdate: datetime.date):
        sql = "SELECT DATE(CreateAt), COUNT(*) FROM comment_table WHERE DATE(CreateAt) BETWEEN '{}' AND '{}' group by DATE(CreateAt) ;".format(
            mindate.strftime("%Y-%m-%d"), maxdate.strftime("%Y-%m-%d")
        )
        self.cursor.execute(sql)
        cnt_per_date = self.cursor.fetchall()

        comment_cnt_dict = {}
        for comment_cnt in cnt_per_date:
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
            "SELECT token, score FROM trend_token_table  WHERE trendAt = '{}' ;".format(
                date.strftime("%Y-%m-%d")
            )
        )
        self.cursor.execute(sql)
        trend_tokens = self.cursor.fetchall()
        trend_tokens_df = pd.DataFrame(trend_tokens)
        return trend_tokens, trend_tokens_df

    def select_similar_tokens(self, date: datetime.date):
        sql = "SELECT token, similar_token_list, score_list FROM similar_token_table  WHERE trendAt = '{}' ;".format(
            date.strftime("%Y-%m-%d")
        )
        self.cursor.execute(sql)
        similar_tokens = self.cursor.fetchall()
        similar_tokens_df = pd.DataFrame(similar_tokens)
        similar_tokens_df = similar_tokens_df.set_index("token")
        return similar_tokens, similar_tokens_df

    def select_tsne(self, date: datetime.date):
        sql = "SELECT token, x, y FROM tsne_table WHERE trendAt = '{}' ;".format(
            date.strftime("%Y-%m-%d")
        )
        self.cursor.execute(sql)
        tsne = self.cursor.fetchall()
        tsne_df = pd.DataFrame(tsne)
        tsne_df = tsne_df.set_index("token")
        return tsne, tsne_df

    def select_trendAt(self):
        sql = "SELECT DATE(trendAt) FROM trend_token_table group by DATE(trendAt)"
        self.cursor.execute(sql)
        trendAt_list = self.cursor.fetchall()
        date_list = []
        for trendAt in trendAt_list:
            date_list.append(trendAt["DATE(trendAt)"])
        return date_list

    def show_isolation_level(self):
        sql = "SHOW VARIABLES LIKE '%isolation';"
        self.cursor.execute(sql)
        isolation_level = self.cursor.fetchall()
        print(isolation_level)
