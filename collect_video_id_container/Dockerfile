FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY scripts/collect_video_id.py /usr/local/bin/collect_video_id
RUN chmod +x /usr/local/bin/collect_video_id

RUN mkdir /comments

ENV PATH="/usr/local/bin:${PATH}"

ENTRYPOINT [ "collect_video_id" ]
CMD [ "--help" ]