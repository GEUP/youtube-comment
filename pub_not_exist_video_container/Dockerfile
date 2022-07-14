FROM python:3.8-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

COPY scripts/pub_not_exist_video.py /usr/local/bin/pub_not_exist_video
RUN chmod +x /usr/local/bin/pub_not_exist_video

RUN mkdir /comments

ENV PATH="/usr/local/bin:${PATH}"

ENTRYPOINT [ "pub_not_exist_video" ]
CMD [ "--help" ]