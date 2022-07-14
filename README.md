# DockerOperator를 활용한 유튜브 댓글 수집

## 아키텍처
![image](https://user-images.githubusercontent.com/33981028/178992808-ac330180-ae4f-4f86-ae62-47838d65c8b3.png)
* 매일 0시에 한국지역 인기동영상을 검색하여 해당 동영상에 달린 댓글을 수집함 (테스트를 위해 최대 6개로 제한)
* 만약 한 번이상 수집된적 있는 동영상인 경우(exist_video) 새로 달린 댓글을 추가하고, 기존에 있던 댓글들 중 수정된 부분(text, likeCount, totalReplyCount, updatedAt)만 찾아서 업데이트함


