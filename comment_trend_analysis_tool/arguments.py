from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class CheckPointArguments:
    CHECKPOINT_BEFORE_EXCEPTION_NEW_COMMENT_PATH: str = field(
        default="./checkpoint/new_comment_checkpoint.pickle", metadata={"help": ""}
    )
    CHECKPOINT_BEFORE_EXCEPTION_EXIST_COMMENT_PATH: str = field(
        default="./checkpoint/exist_comment_checkpoint.pickle", metadata={"help": ""}
    )
    CHECKPOINT_BEFORE_EXCEPTION_COMMENT_DATAFRAME_PATH: str = field(
        default="./checkpoint/comment_df_checkpoint.pickle", metadata={"help": ""}
    )
    CHECKPOINT_BEFORE_EXCEPTION_NEW_RESPONSE_PATH: str = field(
        default="./checkpoint/new_response_checkpoint.pickle", metadata={"help": ""}
    )
    CHECKPOINT_BEFORE_EXCEPTION_EXIST_RESPONSE_PATH: str = field(
        default="./checkpoint/exist_response_checkpoint.pickle", metadata={"help": ""}
    )
    CHECKPOINT_BEFORE_EXCEPTION_REMAIN_NEW_VIDEO_IDS_PATH: str = field(
        default="./checkpoint/remain_new_video_ids_checkpoint.pickle",
        metadata={"help": ""},
    )
    CHECKPOINT_BEFORE_EXCEPTION_REMAIN_EXIST_VIDEO_IDS_PATH: str = field(
        default="./checkpoint/remain_exist_video_ids_checkpoint.pickle",
        metadata={"help": ""},
    )
    CHECKPOINT_GET_DATASET_PATH: str = field(
        default="./checkpoint/get_dataset_checkpoint",
        metadata={"help": ""},
    )
    CHECKPOINT_TREND_RANK_PATH: str = field(
        default="./checkpoint/trend_rank_checkpoint.pickle",
        metadata={"help": ""},
    )
    CHECKPOINT_WORD2VEC_PATH: str = field(
        default="./checkpoint/word2vec_checkpoint",
        metadata={"help": ""},
    )
    CHECKPOINT_TSNE_PATH: str = field(
        default="./checkpoint/tsne_checkpoint",
        metadata={"help": ""},
    )


@dataclass
class TrendArguments:
    max_trend_token: int = field(
        default=5000, metadata={"help": "trend 분석 후 저장할 token의 갯수"}
    )

    re_analysis_min_comment: int = field(
        default=3000,
        metadata={"help": "특정 날짜에 re_analysis_min_comment개의 댓글이 추가되면 다시 트렌드 분석 진행"},
    )

    re_analysis: bool = field(default=False, metadata={"help": "지난 일주일간 트랜드 재분석"})
