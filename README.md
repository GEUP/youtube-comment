# Youtube Most Popular Video Comment Trends V1
## 1. Project Outline
### 프로젝트 목표

* 목적
    * 유튜브 한국 지역 인기 동영상이거나 이였던 영상에 달린 댓글을 분석하여 날짜 별 관심 키워드 추적

### 프로젝트 전체 구조
![project](https://user-images.githubusercontent.com/33981028/157150628-85ddebe4-c968-4cd2-b075-8df5ee33a684.png)


### 결과
![web](https://user-images.githubusercontent.com/33981028/157150983-be6b7adf-323d-499b-a385-d9e91058ed1c.png)

### How to use

* 준비물
    1. Youtube API key
    2. Mysql Database server
* 실행 방법
    1. app에서_사용하는_password_암호화_tutorial.ipynb 파일을 따라서 password.txt를 생성해주세요
	    1) password.txt파일을 comment_trend_analysis_app\comment_trend_analysis\app 과 comment_trend_analysis_tool 폴더에 추가해주세요
    2. comment_trend_analysis_tool\module\DataBase.py 과 comment_trend_analysis_app\comment_trend_analysis\app\db.py 에서 값이 Mysql Database server와 일치 하는지 확인해주세요
        ```python
        self.db = pymysql.connect(
            user="root",
            passwd=db_pw,
            host=db_ip,
            db="comment",
            charset="utf8mb4",
            autocommit=False,
        )
        ```
    3. comment_trend_analysis_tool > python comment_trend.py
    4. comment_trend_analysis_app\comment_trend_analysis > python app

### Software

#### Web Frameworks
- [FastAPI](https://github.com/tiangolo/fastapi)
- [anychart](https://github.com/AnyChart/AnyChart)
- [argon](https://www.creative-tim.com/product/argon-dashboard-django)