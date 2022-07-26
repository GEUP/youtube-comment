FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY scripts/collect_video_comment.py /usr/local/bin/collect_video_comment
RUN chmod +x /usr/local/bin/collect_video_comment

RUN mkdir /comments

ENV PATH="/usr/local/bin:${PATH}"

ENTRYPOINT [ "collect_video_comment" ]
CMD [ "--help" ]