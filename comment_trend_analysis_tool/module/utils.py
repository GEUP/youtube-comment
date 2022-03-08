import requests
import re
import datetime


def send_message_to_slack(msg):
    url = "https://hooks.slack.com/services/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"
    data = {"text": msg}
    resp = requests.post(url=url, json=data)
    return resp


def clean_text(text):
    punctuation = r"[@%\\*=()/~#&\+á?\xc3\xa1\-\|\.\:\;\!\-\,\_\~\$\'\"]"

    review = re.sub(punctuation, "", str(text))  # remove punctuation
    # review = re.sub(r"\d+", "", review)  # remove number
    review = review.lower()  # lower case
    review = re.sub(r"\s+", " ", review)  # remove extra space
    review = re.sub(r"<[^>]+>", "", review)  # remove Html tags
    review = re.sub(r"\s+", " ", review)  # remove spaces
    review = re.sub(r"^\s+", "", review)  # remove space from start
    review = re.sub(r"\s+$", "", review)  # remove space from the end
    return review


def drop_duplicated_comments(comment_df):
    before_drop = len(comment_df)
    comment_df = comment_df.drop_duplicates([0, 1, 2])
    send_message_to_slack("drop {} rows".format(before_drop - len(comment_df)))

    return comment_df


def drop_not_include_korean_comments(comment_df):
    korean = r"[ㄱ-ㅎ|가-힣|ㅏ-ㅣ]"
    condition = comment_df[3].notnull() & comment_df[3].str.contains(korean)
    comment_df = comment_df[condition].reset_index(drop=True)
    return comment_df


def remove_not_korean_alphabet_number(text):
    not_korean_alphabet_number = r"[^A-Z|a-z|0-9|ㄱ-ㅎ|가-힣|ㅏ-ㅣ]"
    return re.sub(not_korean_alphabet_number, "", text)


def remove_stop_pos(okt, token, stop_pos):
    poss = okt.pos(token)
    use_pos = []
    for p in poss:
        if p[1] in stop_pos:
            continue
        else:
            use_pos.append(p[0])
    use_token = "".join(use_pos)
    return use_token


def get_week(queryday):
    week = list()
    for day_ago in range(0, 7):
        week.append((queryday - datetime.timedelta(days=day_ago)).strftime("%Y-%m-%d"))
    week = sorted(week)

    return week


def ㅋㅋ_normalize(token):
    if re.search("ㅋ", token):
        if re.search(r"[^ㅋㄱㄲ]", token):
            return token
        else:
            return "ㅋㅋ"
    return token


def remove_repeat_token(token):
    x = len(token)

    for i in range(1, (x // 2) + 1):
        s = token[0:i]
        count = int(x / i)
        if s * count == token:
            return s

    return token
