# init_db.py
import json
import psycopg2

def main():
    # Читаем файл
    with open('videos.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Получаем список видео
    videos = data['videos']  # <-- ключевой момент!

    conn = psycopg2.connect(
        host="localhost",
        database="video_stats",
        user="user",
        password="password"
    )
    cur = conn.cursor()

    for video in videos:
        # Вставляем видео
        cur.execute("""
            INSERT INTO videos (
                id, creator_id, video_created_at,
                views_count, likes_count, comments_count, reports_count,
                created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING;
        """, (
            video['id'],
            video.get('creator_id'),
            video.get('video_created_at'),
            video.get('views_count'),
            video.get('likes_count'),
            video.get('comments_count'),
            video.get('reports_count'),
            video.get('created_at'),
            video.get('updated_at')
        ))

        # Вставляем снапшоты
        for snap in video.get('snapshots', []):
            cur.execute("""
                INSERT INTO video_snapshots (
                    id, video_id,
                    views_count, likes_count, comments_count, reports_count,
                    delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                snap['id'],
                video['id'],
                snap['views_count'],
                snap['likes_count'],
                snap['comments_count'],
                snap['reports_count'],
                snap['delta_views_count'],
                snap['delta_likes_count'],
                snap['delta_comments_count'],
                snap['delta_reports_count'],
                snap['created_at'],
                snap.get('updated_at', snap['created_at'])
            ))

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Загружено {len(videos)} видео и много снапшотов")

if __name__ == '__main__':
    main()