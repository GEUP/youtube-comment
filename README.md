# DockerOperator를 활용한 유튜브 댓글 수집

## 아키텍처
![image](https://user-images.githubusercontent.com/33981028/177942424-371b5972-aed3-4a79-aebe-97bd80dac42d.png)
* 매일 0시에 한국지역 인기동영상을 검색하여 해당 동영상에 달린 댓글을 수집함 (테스트를 위해 최대 6개로 제한)
* 만약 한 번이상 수집된적 있는 동영상인 경우(exist_video) 새로 달린 댓글을 추가하고, 기존에 있던 댓글들 중 수정된 부분(text, likeCount, totalReplyCount, updatedAt)만 찾아서 업데이트함


## 패키지 설치
![image](https://user-images.githubusercontent.com/33981028/177942169-2fa344f5-6dcc-4732-a187-f24de92d3b51.png)
```
# pip install setuptools
# pip install ./custom_component
```

## Airflow 사용방법
https://geup.tistory.com/26?category=1288080

## RabbitMQ 사용방법
https://geup.tistory.com/28?category=1289921
