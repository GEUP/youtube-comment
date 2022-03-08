import pymysql
import pandas as pd
import os
import cryptocode
from tqdm import tqdm

from module.utils import clean_text, send_message_to_slack

import datetime


class DataBase:
    def __init__(self, week):

        self.week = week

        with open("./password.txt", "r") as f:
            password = cryptocode.decrypt(f.read(), os.environ["master_key"]).split(
                "\n"
            )
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

    def set_week(self, week):
        self.week = week

    def select_six_day_comment(self):
        sql = "SELECT comment FROM comment_table WHERE DATE(CreateAt) BETWEEN '{}' AND '{}' ;".format(
            self.week[0], self.week[-2]
        )
        self.cursor.execute(sql)
        six_day_result = self.cursor.fetchall()
        six_day_result_df = pd.DataFrame(six_day_result)

        return six_day_result, six_day_result_df

    def select_queryday_comment(self):
        sql = "SELECT comment FROM comment_table WHERE DATE(CreateAt) = '{}' ;".format(
            self.week[-1]
        )
        self.cursor.execute(sql)
        queryday_result = self.cursor.fetchall()
        queryday_result_df = pd.DataFrame(queryday_result)

        return queryday_result, queryday_result_df

    def init_queryday_trend_token(self):
        sql = "DELETE FROM trend_token_table WHERE trendAt  = '{}' ;".format(
            self.week[-1]
        )
        self.cursor.execute(sql)
        self.cursor.fetchall()

    def insert_trend_token(self, sorted_queryday_token_cnt_df):
        try:
            for di, row in enumerate(
                tqdm(
                    sorted_queryday_token_cnt_df.iterrows(),
                    total=len(sorted_queryday_token_cnt_df),
                )
            ):
                sql = "INSERT INTO trend_token_table(token, score, trendAt) VALUES ('{}',{},'{}');".format(
                    row[1]["token"], row[1]["score"], self.week[-1]
                )
                self.cursor.execute(sql)
                if (di % 10000) == 0:
                    self.db.commit()
        except Exception as e:
            send_message_to_slack(sql)
            send_message_to_slack(str(e))
            raise
        self.db.commit()

    def init_queryday_similar_token(self):
        sql = "DELETE FROM similar_token_table WHERE trendAt  = '{}' ;".format(
            self.week[-1]
        )
        self.cursor.execute(sql)
        self.cursor.fetchall()

    def insert_similar_token(self, similar_tokens_df):
        try:
            for di, row in enumerate(
                tqdm(similar_tokens_df.iterrows(), total=len(similar_tokens_df))
            ):
                sql = "INSERT INTO similar_token_table(token, similar_token_list, score_list, trendAt) VALUES ('{}','[{}]','[{}]','{}');".format(
                    row[0],
                    ", ".join(row[1]["similar_token_list"]),
                    ", ".join([str(score) for score in row[1]["score_list"]]),
                    self.week[-1],
                )
                self.cursor.execute(sql)
                if (di % 10000) == 0:
                    self.db.commit()
        except Exception as e:
            send_message_to_slack(sql)
            send_message_to_slack(str(e))
            raise
        self.db.commit()

    def init_queryday_tsne(self):
        sql = "DELETE FROM tsne_table WHERE trendAt  = '{}' ;".format(self.week[-1])
        self.cursor.execute(sql)
        self.cursor.fetchall()

    def insert_tsne(self, tsne_df):
        try:
            for di, row in enumerate(tqdm(tsne_df.iterrows(), total=len(tsne_df))):
                sql = "INSERT INTO tsne_table(token, x, y, trendAt) VALUES ('{}',{},{},'{}');".format(
                    row[0], row[1]["x"], row[1]["y"], self.week[-1]
                )
                self.cursor.execute(sql)
                if (di % 10000) == 0:
                    self.db.commit()
        except Exception as e:
            send_message_to_slack(sql)
            send_message_to_slack(str(e))
            raise
        self.db.commit()

    def delete_not_found_video_comment(self, video_id):
        sql = "DELETE FROM comment_table WHERE video_id  = '{}' ;".format(video_id)
        self.cursor.execute(sql)
        self.cursor.fetchall()

    def select_exist_video_and_max_date(self):
        sql = "SELECT video_id, DATE(MAX(DATE(CreateAt))), MAX(top_id) FROM comment_table GROUP BY video_id;"
        self.cursor.execute(sql)
        select_result = self.cursor.fetchall()
        return select_result

    def insert_comment(self, checkpoint_manager, comment_df):
        try:
            publish_at_cnt = {}
            for di, x in enumerate(tqdm(comment_df.iterrows(), total=len(comment_df))):
                createAt = x[1][5].replace("T", " ").replace("Z", "")
                sql = "INSERT INTO comment_table(video_id, top_id, bottom_id, comment, author, createAt, num_likes, sentiment) VALUES ('{}',{},{},'{}','{}','{}',{},{});".format(
                    x[1][0],
                    x[1][1],
                    x[1][2],
                    x[1][3],
                    clean_text(x[1][4]),
                    createAt,
                    x[1][6],
                    0,
                )
                self.cursor.execute(sql)

                createAt = datetime.datetime.strptime(
                    createAt.split(" ")[0], "%Y-%m-%d"
                ).date()

                if createAt in publish_at_cnt:
                    publish_at_cnt[createAt] += 1
                else:
                    publish_at_cnt[createAt] = 1

                if (di % 10000) == 0:
                    self.db.commit()
                    checkpoint_manager.save_comment_dataframe_checkpoint(
                        comment_df[di + 1 :]
                    )
        except Exception as e:
            send_message_to_slack(sql)
            send_message_to_slack(str(e))
            raise
        self.db.commit()
        return publish_at_cnt

    def close(self):
        self.db.close()
