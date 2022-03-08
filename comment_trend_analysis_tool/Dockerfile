FROM ubuntu:latest

ARG master_key

COPY . /comment_trend_analysis_tool
WORKDIR /comment_trend_analysis_tool
ENV PYTHONPATH=/comment_trend_analysis_tool
ENV PYTHONUNBUFFERED=1
ENV master_key=${master_key}

# apt init
ENV LANG=C.UTF-8
ENV TZ=Asia/Seoul
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata g++ git curl

# java stuff
RUN apt-get install -y openjdk-8-jdk

# python stuff
RUN apt-get install -y python3-pip python3-dev
RUN cd /usr/local/bin && \
    ln -s /usr/bin/python3 python && \
    ln -s /usr/bin/pip3 pip && \
    pip3 install --upgrade pip

# apt cleanse
RUN apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# timezone
RUN ln -sf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

RUN pip install --upgrade pip
RUN pip install -r ./requirements.txt

CMD ["python", "comment_trend.py"]
