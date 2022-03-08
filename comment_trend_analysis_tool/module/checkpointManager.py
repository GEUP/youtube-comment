import pickle
import os

from defusedxml import DTDForbidden


class checkpointManager:
    def __init__(self, checkpoint_args):
        self.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH
        )
        self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH
        )
        self.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH
        )
        self.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH
        )
        self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH
        )
        self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH
        )
        self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH = (
            checkpoint_args.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH
        )
        self.CHECKPOINT_GET_DATASET_PATH = checkpoint_args.CHECKPOINT_GET_DATASET_PATH
        self.CHECKPOINT_TREND_RANK_PATH = checkpoint_args.CHECKPOINT_TREND_RANK_PATH
        self.CHECKPOINT_WORD2VEC_PATH = checkpoint_args.CHECKPOINT_WORD2VEC_PATH
        self.CHECKPOINT_TSNE_PATH = checkpoint_args.CHECKPOINT_TSNE_PATH

    def new_video_ids_checkpoint_exist(self):
        return os.path.exists(
            self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH
        )

    def read_remain_new_video_ids_checkpoint(self):

        with open(
            self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH, "rb"
        ) as rf:
            most_popular_video_id_and_title_list = pickle.load(rf)

        return most_popular_video_id_and_title_list

    def new_video_response_checkpoint_exist(self):
        return os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH)

    def read_new_video_response_checkpoint(self):
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH, "rb") as rf:
            response = pickle.load(rf)

        return response

    def save_new_video_checkpoint(
        self, response, most_popular_video_id_and_title_list, comments
    ):
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH, "wb") as fw:
            pickle.dump(response, fw)
        with open(
            self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH, "wb"
        ) as fw:
            pickle.dump(most_popular_video_id_and_title_list, fw)
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH, "wb") as fw:
            pickle.dump(comments, fw)

    #############################################################################
    def exist_video_ids_checkpoint_exist(self):
        return os.path.exists(
            self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH
        )

    def read_remain_exist_video_ids_checkpoint(self):

        with open(
            self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH, "rb"
        ) as rf:
            video_ids = pickle.load(rf)

        return video_ids

    def exist_video_response_checkpoint_exist(self):
        return os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH)

    def read_exist_video_response_checkpoint(self):
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH, "rb") as rf:
            response = pickle.load(rf)
        return response

    def save_exist_video_checkpoint(self, response, video_ids, comments):
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH, "wb") as fw:
            pickle.dump(response, fw)
        with open(
            self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH, "wb"
        ) as fw:
            pickle.dump(video_ids, fw)
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH, "wb") as fw:
            pickle.dump(comments, fw)

    def exist_comment_dataframe_checkpoint(self):
        return os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH)

    def read_comment_dataframe_checkpoint(self):
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH, "rb") as rf:
            comment_df = pickle.load(rf)
        return comment_df

    def save_comment_dataframe_checkpoint(self, comment_df):
        with open(self.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH, "wb") as fw:
            pickle.dump(comment_df, fw)

    def read_new_comment_checkpoint(self):
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH):
            with open(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH, "rb") as rf:
                new_comments = pickle.load(rf)
            return new_comments
        else:
            return list()

    def read_exist_comment_checkpoint(self):
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH):
            with open(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH, "rb") as rf:
                exist_comments = pickle.load(rf)
            return exist_comments
        else:
            return list()

    def remove_every_comment_checkpoint(self):
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH)
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH)
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH)
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH)
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH)
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH)
        if os.path.exists(self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH):
            os.remove(self.CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH)

    #############################################################################
    def exist_get_dataset_checkpoint(self):
        return os.path.exists(self.CHECKPOINT_GET_DATASET_PATH)

    def save_get_dataset_checkpoint(self):
        self.CHECKPOINT_GET_DATASET_PATH
        with open(self.CHECKPOINT_GET_DATASET_PATH, "w") as fw:
            fw.write("")

    #############################################################################
    def exist_trend_rank_checkpoint(self):
        return os.path.exists(self.CHECKPOINT_TREND_RANK_PATH)

    def save_trend_rank_checkpoint(self, ranked_tokens, sentence_tokens_for_word2vec):
        self.CHECKPOINT_TREND_RANK_PATH
        with open(self.CHECKPOINT_TREND_RANK_PATH, "wb") as fw:
            pickle.dump((ranked_tokens, sentence_tokens_for_word2vec), fw)

    def read_trend_rank_checkpoint(self):
        with open(self.CHECKPOINT_TREND_RANK_PATH, "rb") as rf:
            ranked_tokens, sentence_tokens_for_word2vec = pickle.load(rf)
        return ranked_tokens, sentence_tokens_for_word2vec

    #############################################################################
    def exist_word2vec_checkpoint(self):
        return os.path.exists(self.CHECKPOINT_WORD2VEC_PATH)

    def save_word2vec_checkpoint(self):
        self.CHECKPOINT_WORD2VEC_PATH
        with open(self.CHECKPOINT_WORD2VEC_PATH, "w") as fw:
            fw.write("")

    #############################################################################
    def exist_tsne_checkpoint(self):
        return os.path.exists(self.CHECKPOINT_TSNE_PATH)

    def save_tsne_checkpoint(self):
        self.CHECKPOINT_TSNE_PATH
        with open(self.CHECKPOINT_TSNE_PATH, "w") as fw:
            fw.write("")

    #############################################################################
    def remove_every_trend_checkpoint(self):
        if os.path.exists(self.CHECKPOINT_GET_DATASET_PATH):
            os.remove(self.CHECKPOINT_GET_DATASET_PATH)
        if os.path.exists(self.CHECKPOINT_TREND_RANK_PATH):
            os.remove(self.CHECKPOINT_TREND_RANK_PATH)
        if os.path.exists(self.CHECKPOINT_WORD2VEC_PATH):
            os.remove(self.CHECKPOINT_WORD2VEC_PATH)
        if os.path.exists(self.CHECKPOINT_TSNE_PATH):
            os.remove(self.CHECKPOINT_TSNE_PATH)
