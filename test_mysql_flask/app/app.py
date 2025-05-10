import os
import pymysql
import numpy as np
from flask import Flask, request, jsonify, render_template
from datetime import datetime
from tensorflow.keras.models import load_model
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

# DB 설정
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# DB 연결
def get_connection():
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME,
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

# 모델 로드
model = load_model("model_gru.h5")

# 테이블 생성
def create_table():
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS time_series_data")
            cols = ", ".join([f"feature{i} FLOAT" for i in range(1, 52)])
            cur.execute(f"""
                CREATE TABLE time_series_data (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    {cols}
                )
            """)
        conn.commit()

@app.route('/')
def index():
    return render_template('upload.html')

@app.route('/upload', methods=['POST'])
def upload_npy():
    file = request.files.get('file')
    if not file or not file.filename.endswith('.npy'):
        return jsonify({'error': 'Only .npy files are accepted.'}), 400

    data = np.load(file)
    if data.ndim != 2 or data.shape[1] != 51:
        return jsonify({'error': f'Expected shape (N, 51), got {data.shape}'}), 400

    # MySQL 저장
    with get_connection() as conn:
        with conn.cursor() as cur:
            for row in data:
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                placeholders = ", ".join(["%s"] * 52)
                sql = f"""
                INSERT INTO time_series_data (timestamp, {', '.join([f'feature{i}' for i in range(1, 52)])})
                VALUES ({placeholders})
                """
                cur.execute(sql, (timestamp, *row.tolist()))
        conn.commit()

    # 모델 예측
    prediction = model.predict(data)
    return jsonify({'prediction': prediction.tolist()})

@app.route('/init')
def init_db():
    create_table()
    return "✅ 테이블 초기화 완료!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
