import json
import psycopg2
from datetime import datetime

# Подключение к БД
conn = psycopg2.connect(
    host="localhost",
    database="video_stats",
    user="user",
    password="password"
)
cur = conn.cursor()

# Загрузка JSON
with open('videos.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Вставка данных
for item in data:
    # Основное видео
    cur.execute("""
        INSERT INTO videos VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
    """, (
        item['id'],
        item['creator_id'],
        item['video_created_at'],
        item['views_count'],
        item['likes_count'],
        item['comments_count'],
        item['reports_count'],
        item['created_at'],
        item['updated_at']
    ))

    # Снапшоты
    for snap in item.get('snapshots', []):
        cur.execute("""
            INSERT INTO video_snapshots VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            snap['id'],
            item['id'],  # video_id
            snap['views_count'],
            snap['likes_count'],
            snap['comments_count'],
            snap['reports_count'],
            snap['delta_views_count'],
            snap['delta_likes_count'],
            snap['delta_comments_count'],
            snap['delta_reports_count'],
            snap['created_at'],
            snap['created_at'],  # updated_at
            snap['created_at']   # временно
        ))

conn.commit()
cur.close()
conn.close()
print("Данные загружены.")