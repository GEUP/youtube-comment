FROM python:3.8.7-slim-buster

ARG master_key

COPY . /comment_trend_analysis_app
WORKDIR /comment_trend_analysis_app/comment_trend_analysis
ENV PYTHONPATH=/comment_trend_analysis_app/comment_trend_analysis
ENV PYTHONUNBUFFERED=1
ENV master_key=${master_key}

EXPOSE 8080

RUN pip install --upgrade pip
RUN pip install -r ../requirements.txt

CMD ["python", "app"]
