import os
import random
from datetime import datetime, timedelta
import pymysql
from flask import Flask

app = Flask(__name__)

# DB 연결 정보
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')


def insert_test_data():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
    cur = conn.cursor()

    start_time = datetime.now() - timedelta(days=10)
    for _ in range(100000):  # 약 10MB 분량 데이터 (필드 수, row 수로 조절 가능)
        timestamp = start_time.strftime('%Y-%m-%d %H:%M:%S')
        values = (
            timestamp,
            round(random.uniform(0, 100), 2),
            round(random.uniform(0, 100), 2),
            round(random.uniform(0, 100), 2),
            round(random.uniform(0, 100), 2),
        )
        cur.execute(
            "INSERT INTO time_series_data (timestamp, feature1, feature2, feature3, feature4) VALUES (%s, %s, %s, %s, %s)", values
        )
        start_time += timedelta(seconds=1)

    conn.commit()
    cur.close()
    conn.close()


@app.route('/insert')
def insert():
    insert_test_data()
    return "10MB 테스트 데이터 삽입 완료"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
