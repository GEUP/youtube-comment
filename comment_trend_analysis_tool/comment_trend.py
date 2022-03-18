import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tqdm import tqdm
import datetime
from soynlp import DoublespaceLineCorpus
from soynlp.word import WordExtractor
from soynlp.tokenizer import MaxScoreTokenizer
from soynlp.normalizer import *
from konlpy.tag import Okt
from gensim.models import Word2Vec
import gensim.models as g
from sklearn.manifold import TSNE

from transformers import HfArgumentParser
from arguments import CheckPointArguments, TrendArguments

from module.checkpointManager import checkpointManager
from module.DataBase import DataBase
from module.commentCollector import commentCollector
from module.utils import (
    remove_not_korean_alphabet_number,
    remove_stop_pos,
    clean_text,
    drop_duplicated_comments,
    drop_not_include_korean_comments,
    get_week,
    ㅋㅋ_normalize,
    remove_repeat_token,
)


def get_new_video_comment(
    comment_collector, exist_video_ids, week, queryday, checkpoint_manager
):
    # 처음 수집하는 인기 동영상들의 댓글을 수집한다.
    maxResults = comment_collector.maxResults
    comments = list()

    most_popular_video_id_and_title_list = list()
    if checkpoint_manager.new_video_ids_checkpoint_exist():
        most_popular_video_id_and_title_list = (
            checkpoint_manager.read_remain_new_video_ids_checkpoint()
        )
    else:
        most_popular_video_id_and_title_list = (
            comment_collector.request_mostPopular_videos()
        )

    if most_popular_video_id_and_title_list == []:
        return []

    video_ids, titles = list(zip(*most_popular_video_id_and_title_list))
    video_ids = [video_id for video_id in video_ids if video_id not in exist_video_ids]

    with tqdm(video_ids) as t:
        for vi, video_id in enumerate(t):
            t.set_description(f"[video : {vi}]")
            
            if checkpoint_manager.new_video_response_checkpoint_exist():
                response = checkpoint_manager.read_new_video_response_checkpoint()
            else:
                try:
                    response = comment_collector.request_comment(video_id)
                except HttpError as e:
                    if e.resp.status in [403, 400]:
                        continue
                    else:
                        raise

            try:
                transaction_comments = []
                next = 0
                while response:
                    for i, item in enumerate(response["items"]):
                        t.set_postfix(top_id=f"{next*maxResults+i}")
                        comment = item["snippet"]["topLevelComment"]["snippet"]

                        if comment["publishedAt"].split("T")[0] not in week:
                            continue

                        publish_at = datetime.datetime.strptime(
                            comment["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                        ).date()

                        transaction_comments.append(
                            [
                                video_id,
                                next * maxResults + i,
                                0,
                                clean_text(comment["textOriginal"]),
                                comment["authorDisplayName"],
                                comment["publishedAt"],
                                comment["likeCount"],
                            ]
                        )

                        if item["snippet"]["totalReplyCount"] > 0:
                            if "replies" in item:
                                for j, reply_item in enumerate(
                                    item["replies"]["comments"]
                                ):
                                    reply = reply_item["snippet"]
                                    transaction_comments.append(
                                        [
                                            video_id,
                                            next * maxResults + i,
                                            j + 1,
                                            clean_text(reply["textOriginal"]),
                                            reply["authorDisplayName"],
                                            reply["publishedAt"],
                                            reply["likeCount"],
                                        ]
                                    )

                    if "nextPageToken" in response:
                        try:
                            response = comment_collector.request_next_comment(
                                video_id, response
                            )
                            next += 1
                        except HttpError as e:
                            if e.resp.status in [403]:
                                break
                            else:
                                raise
                    else:
                        break

            except Exception as e:
                checkpoint_manager.save_new_video_checkpoint(
                    response, most_popular_video_id_and_title_list[vi:], comments
                )
                raise
            comments.extend(transaction_comments)

    checkpoint_manager.save_new_video_checkpoint(None, [], comments)

    return comments


def get_exist_video_comment(
    comment_collector, exist_video_and_max_date, queryday, checkpoint_manager
):
    # 이미 수집한 영상들 중 새로 댓글이 추가된 영상의 댓글을 수집한다.
    maxResults = comment_collector.maxResults
    comments = list()

    if checkpoint_manager.exist_video_ids_checkpoint_exist():
        video_ids = checkpoint_manager.read_remain_exist_video_ids_checkpoint()
    else:
        need_to_get_comment_date = {}
        for video_saved_max_date in exist_video_and_max_date:
            if video_saved_max_date["DATE(MAX(DATE(CreateAt)))"] < queryday:
                need_to_get_comment_date[video_saved_max_date["video_id"]] = {
                    "max_saved_day": video_saved_max_date["DATE(MAX(DATE(CreateAt)))"],
                    "max_top_id": video_saved_max_date["MAX(top_id)"],
                }
        video_ids = need_to_get_comment_date.keys()

    with tqdm(video_ids) as t:
        for vi, video_id in enumerate(t):
            t.set_description(f"[video : {vi}]")
            if checkpoint_manager.exist_video_response_checkpoint_exist():
                response = checkpoint_manager.read_exist_video_response_checkpoint()

            else:
                try:
                    response = comment_collector.request_comment(video_id)
                except HttpError as e:
                    if e.resp.status in [403, 404]:
                        continue

                    else:
                        raise

            try:
                transaction_comments = []
                next = 0
                while response:
                    for i, item in enumerate(response["items"]):
                        max_top_id = (
                            need_to_get_comment_date[video_id]["max_top_id"] + 1
                        )
                        t.set_postfix(top_id=f"{next*maxResults+i+max_top_id}")
                        comment = item["snippet"]["topLevelComment"]["snippet"]
                        publish_at = datetime.datetime.strptime(
                            comment["publishedAt"], "%Y-%m-%dT%H:%M:%SZ"
                        ).date()

                        if publish_at > queryday:
                            continue
                        elif (
                            publish_at
                            <= need_to_get_comment_date[video_id]["max_saved_day"]
                        ):
                            response = []
                            break

                        transaction_comments.append(
                            [
                                video_id,
                                next * maxResults + i + max_top_id,
                                0,
                                clean_text(comment["textOriginal"]),
                                comment["authorDisplayName"],
                                comment["publishedAt"],
                                comment["likeCount"],
                            ]
                        )

                        if item["snippet"]["totalReplyCount"] > 0:
                            if "replies" in item:
                                for j, reply_item in enumerate(
                                    item["replies"]["comments"]
                                ):
                                    reply = reply_item["snippet"]
                                    transaction_comments.append(
                                        [
                                            video_id,
                                            next * maxResults + i + max_top_id,
                                            j + 1,
                                            clean_text(reply["textOriginal"]),
                                            reply["authorDisplayName"],
                                            reply["publishedAt"],
                                            reply["likeCount"],
                                        ]
                                    )

                    if "nextPageToken" in response:
                        try:
                            response = comment_collector.request_next_comment(
                                video_id, response
                            )
                            next += 1
                        except HttpError as e:
                            if e.resp.status in [403]:
                                break
                            else:
                                raise
                    else:
                        break

            except Exception as e:
                checkpoint_manager.save_exist_video_checkpoint(
                    response, video_ids[vi:], comments
                )
                raise

            comments.extend(transaction_comments)

    checkpoint_manager.save_exist_video_checkpoint(None, [], comments)

    return comments


def get_dataset(
    comment_collector, week, queryday, database, checkpoint_manager, trend_args
):
    # youtube api를 활용해 인기동영상 댓글들을 수집한다.
    if checkpoint_manager.exist_comment_dataframe_checkpoint():
        comment_df = checkpoint_manager.read_comment_dataframe_checkpoint()
    else:
        new_comments = checkpoint_manager.read_new_comment_checkpoint()
        exist_comments = checkpoint_manager.read_exist_comment_checkpoint()

        exist_video_and_max_date = database.select_exist_video_and_max_date()

        exist_video_ids = list()
        if exist_video_and_max_date != []:
            exist_video_ids = list(pd.DataFrame(exist_video_and_max_date)["video_id"])
            print("get_exist_video_comment")
            temp_comments = get_exist_video_comment(
                comment_collector,
                exist_video_and_max_date,
                queryday,
                checkpoint_manager,
            )
            exist_comments.extend(temp_comments)

        print("get_new_video_comment")
        temp_comments = get_new_video_comment(
            comment_collector, exist_video_ids, week, queryday, checkpoint_manager
        )
        new_comments.extend(temp_comments)

        
        exist_comments_df = pd.DataFrame(exist_comments)
        exist_comments_df = drop_duplicated_comments(exist_comments_df)

        new_comments_df = pd.DataFrame(new_comments)
        new_comments_df = drop_duplicated_comments(new_comments_df)

        comment_df = pd.concat([exist_comments_df, new_comments_df])
        comment_df = drop_not_include_korean_comments(comment_df)
        comment_df = drop_duplicated_comments(comment_df)

    print("start INSERT INTO comment_table")
    publish_at_cnt = database.insert_comment(checkpoint_manager, comment_df)

    print("add comment At : ", publish_at_cnt)
    print("re analysis added over 3,000 comment date")
    for querydate, cnt in publish_at_cnt.items():
        if querydate == queryday:
            continue
        if cnt >= trend_args.re_analysis_min_comment:
            get_queryday_trend(
                database=database,
                checkpoint_manager=checkpoint_manager,
                queryday=querydate,
                trend_args=trend_args,
            )

    checkpoint_manager.remove_every_comment_checkpoint()
    checkpoint_manager.save_get_dataset_checkpoint()


def get_tokens(maxscore_tokenizer, okt, stop_pos, sql_result):
    # 수집된 댓글들을 토큰화한다.
    token_cnt = {}
    sentence_tokens_for_word2vec = []
    for sentence in tqdm(sql_result):
        tokens = maxscore_tokenizer.tokenize(
            emoticon_normalize(sentence["comment"], num_repeats=2)
        )
        cleaned_tokens = []

        for token in tokens:
            removed_token = remove_stop_pos(okt, token, stop_pos)
            removed_token = remove_not_korean_alphabet_number(removed_token)
            removed_token = ㅋㅋ_normalize(removed_token)
            removed_token = remove_repeat_token(removed_token)
            if (removed_token == "") or (len(removed_token) == 1):
                continue

            cleaned_tokens.append(removed_token)

            if removed_token in token_cnt:
                token_cnt[removed_token] += 1
            else:
                token_cnt[removed_token] = 1

        if cleaned_tokens != []:
            sentence_tokens_for_word2vec.append(cleaned_tokens)

    return token_cnt, sentence_tokens_for_word2vec


def get_token_trend_ranking(database, trend_args):
    """
    1. database에서 수집된 댓글을 가져와서 tokenizer를 학습시키고
    2. 수집된 댓글을 토큰화하여
    3. 각 토큰을 트렌드 점수를 기준으로 ranking한다.

    return 토큰 순위, 토큰화된 댓글들
    """

    okt = Okt()
    stop_pos = ["Eomi", "Josa", "Alpha", "Foreign", "Adverb", "Adjective", "Verb"]

    # select comment
    six_day_result, six_day_result_df = database.select_six_day_comment()
    if len(six_day_result)<=0:
        return "NOTKENSIXDAY", "NOTKENSIXDAY"
    queryday_result, queryday_result_df = database.select_queryday_comment()
    week_result = pd.concat([six_day_result_df, queryday_result_df]).reset_index(
        drop=True
    )

    # train tokenizer
    week_corpus = []
    for comment in week_result["comment"]:
        week_corpus.append(comment)
    corpus_str = "\n".join(week_corpus)
    with open("corpus.txt", "w", encoding="utf8") as f:
        f.write(corpus_str)
    corpus = DoublespaceLineCorpus("corpus.txt")
    word_extractor = WordExtractor()
    word_extractor.train(corpus)
    word_score_table = word_extractor.extract()
    scores = {word: score.cohesion_forward for word, score in word_score_table.items()}
    maxscore_tokenizer = MaxScoreTokenizer(scores=scores)

    # make sentence tokens list for word2vec
    sentence_tokens_for_word2vec = []
    six_day_token_cnt, six_day_sentence_tokens_for_word2vec = get_tokens(
        maxscore_tokenizer, okt, stop_pos, six_day_result
    )
    queryday_token_cnt, queryday_sentence_tokens_for_word2vec = get_tokens(
        maxscore_tokenizer, okt, stop_pos, queryday_result
    )
    sentence_tokens_for_word2vec.extend(six_day_sentence_tokens_for_word2vec)
    sentence_tokens_for_word2vec.extend(queryday_sentence_tokens_for_word2vec)

    # rank trendy tokens
    weight = len(queryday_result) / (len(six_day_result))
    for x in queryday_token_cnt.keys():
        if x in six_day_token_cnt:
            queryday_token_cnt[x] -= weight * six_day_token_cnt[x]

    ranked_queryday_tokens = sorted(
        queryday_token_cnt.items(), key=lambda x: x[1], reverse=True
    )

    ranked_queryday_tokens_df = pd.DataFrame(
        ranked_queryday_tokens, columns=["token", "score"]
    )


    ranked_queryday_tokens_df = ranked_queryday_tokens_df.head(
        trend_args.max_trend_token
    )

    print("start INSERT INTO trend_token_table")
    database.insert_trend_token(ranked_queryday_tokens_df)

    ranked_tokens, _ = zip(*ranked_queryday_tokens)

    return ranked_tokens, sentence_tokens_for_word2vec


def word2vec_analysis(
    database,
    ranked_tokens,
    sentence_tokens_for_word2vec,
    trend_args,
    model_name="1minwords",
):
    """
    토큰들의 순위를 사용자가 쉽게 확인 할 수 있도록 Word2Vec과 t-SNE를 활용해 분석한다.
    """
    print(len(ranked_tokens))
    # train Word2Vec
    model = Word2Vec(sentence_tokens_for_word2vec, window=4, min_count=1, epochs=5)
    model.save(model_name)

    # get similar tokens
    similar_tokens = []
    for t in tqdm(ranked_tokens):
        similar_token, score = zip(*model.wv.most_similar(t))
        similar_tokens.append([t, similar_token, score])

    similar_tokens_df = pd.DataFrame(
        similar_tokens, columns=["token", "similar_token_list", "score_list"]
    )
    similar_tokens_df = similar_tokens_df.set_index("token")


    similar_tokens_df = similar_tokens_df.head(trend_args.max_trend_token)
    print("start INSERT INTO similar_token_table")
    database.insert_similar_token(similar_tokens_df)


def tsne_analysis(ranked_tokens, trend_args, model_name="1minwords"):
    # dimension reduction ranked tokens
    model = g.Doc2Vec.load(model_name)
    X = model.wv[ranked_tokens]
    tsne = TSNE(n_components=2)
    max_size = len(X)
    X_tsne = tsne.fit_transform(X[:max_size, :])

    tsne_df = pd.DataFrame(X_tsne, index=ranked_tokens[:max_size], columns=["x", "y"])


    tsne_df = tsne_df.head(trend_args.max_trend_token)
    print("start INSERT INTO tsne_table")
    database.insert_tsne(tsne_df)


def get_trend(database, checkpoint_manager, trend_args):
    if not checkpoint_manager.exist_trend_rank_checkpoint():
        ranked_tokens, sentence_tokens_for_word2vec = get_token_trend_ranking(
            database, trend_args
        )
        if ranked_tokens == "NOTKENSIXDAY":
            return 
        checkpoint_manager.save_trend_rank_checkpoint(
            ranked_tokens, sentence_tokens_for_word2vec
        )
    else:
        (
            ranked_tokens,
            sentence_tokens_for_word2vec,
        ) = checkpoint_manager.read_trend_rank_checkpoint()

    if not checkpoint_manager.exist_word2vec_checkpoint():
        word2vec_analysis(
            database, ranked_tokens, sentence_tokens_for_word2vec, trend_args
        )
        checkpoint_manager.save_word2vec_checkpoint()

    if not checkpoint_manager.exist_tsne_checkpoint():
        tsne_analysis(ranked_tokens, trend_args)
    else:
        checkpoint_manager.save_tsne_checkpoint()

    checkpoint_manager.remove_every_trend_checkpoint()


def get_queryday_trend(database, checkpoint_manager, queryday, trend_args):
    print("analysis trend At :", queryday)
    query_week = get_week(queryday)
    old_week = database.week
    database.set_week(query_week)
    get_trend(database, checkpoint_manager, trend_args)
    database.set_week(old_week)


if __name__ == "__main__":

    parser = HfArgumentParser((CheckPointArguments, TrendArguments))
    checkpoint_args, trend_args = parser.parse_args_into_dataclasses()

    checkpoint_manager = checkpointManager(checkpoint_args)

    comment_collector = commentCollector()

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).date()
    week = get_week(queryday=yesterday)

    database = DataBase(week)

    if trend_args.re_analysis:
        queryday = (datetime.datetime.now() - datetime.timedelta(days=2)).date()
        week = get_week(queryday=queryday)
        for d in week:
            d = datetime.datetime.strptime(d, "%Y-%m-%d").date()
            get_queryday_trend(
                database=database,
                checkpoint_manager=checkpoint_manager,
                queryday=d,
                trend_args=trend_args,
            )
    else:
        try:
            if not checkpoint_manager.exist_get_dataset_checkpoint():
                get_dataset(
                    comment_collector=comment_collector,
                    week=week,
                    queryday=yesterday,
                    database=database,
                    checkpoint_manager=checkpoint_manager,
                    trend_args=trend_args,
                )

            get_queryday_trend(
                database=database,
                checkpoint_manager=checkpoint_manager,
                queryday=yesterday,
                trend_args=trend_args,
            )
        except Exception as e:
            raise
        finally:
            database.close()
