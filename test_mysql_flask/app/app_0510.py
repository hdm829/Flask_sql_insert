import os
import random
from datetime import datetime, timedelta
import pymysql
from flask import Flask
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# DB 연결 정보
DB_HOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_NAME')

# 51개 feature용 테이블 생성 함수
def create_table():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
    cur = conn.cursor()

    cur.execute("DROP TABLE IF EXISTS time_series_data")  # 기존 테이블 삭제 (선택사항)

    columns = ", ".join([f"feature{i} FLOAT" for i in range(1, 52)])
    create_sql = f"""
    CREATE TABLE time_series_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        timestamp DATETIME,
        {columns}
    )
    """
    cur.execute(create_sql)
    conn.commit()
    cur.close()
    conn.close()


# 더미 데이터 삽입
def insert_test_data():
    conn = pymysql.connect(host=DB_HOST, user=DB_USER, password=DB_PASSWORD, db=DB_NAME)
    cur = conn.cursor()

    start_time = datetime.now() - timedelta(days=10)
    for _ in range(100000):
        timestamp = start_time.strftime('%Y-%m-%d %H:%M:%S')
        features = [round(random.uniform(0, 100), 2) for _ in range(51)]
        values = (timestamp, *features)
        placeholders = ", ".join(["%s"] * (1 + 51))
        sql = f"INSERT INTO time_series_data (timestamp, {', '.join([f'feature{i}' for i in range(1, 52)])}) VALUES ({placeholders})"
        cur.execute(sql, values)
        start_time += timedelta(seconds=1)

    conn.commit()
    cur.close()
    conn.close()


@app.route('/insert')
def insert():
    create_table()  # 테이블 먼저 생성
    insert_test_data()
    return "✅ 10만 개의 더미 데이터 삽입 완료!"


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
