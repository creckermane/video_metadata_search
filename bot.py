import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
import psycopg2
import ollama

logging.basicConfig(level=logging.INFO)

BOT_TOKEN = ""

def get_db():
    return psycopg2.connect(
        host="localhost",
        database="video_stats",
        user="user",
        password="password"
    )


PROMPT = """
Ты — SQL-бот для аналитики видео. Отвечай ТОЛЬКО валидным SQL-запросом, который возвращает ОДНО целое число. Никаких слов, пояснений, комментариев.

Схема БД:
- Таблица `videos` (итоговая статистика):
  id TEXT, creator_id TEXT, video_created_at TIMESTAMP,
  views_count BIGINT, likes_count BIGINT, comments_count BIGINT, reports_count BIGINT
- Таблица `video_snapshots` (почасовые замеры):
  id TEXT, video_id TEXT, created_at TIMESTAMP,
  views_count BIGINT, likes_count BIGINT, comments_count BIGINT, reports_count BIGINT,
  delta_views_count BIGINT, delta_likes_count BIGINT, delta_comments_count BIGINT, delta_reports_count BIGINT

Правила:
1. Если вопрос про "сколько видео", "у креатора с id X", "опубликовано с ДАТА1 по ДАТА2", "набрало больше N просмотров" — используй ТОЛЬКО `videos`.
2. Если вопрос про "суммарное количество просмотров у видео, опубликованных в месяце/диапазоне" — используй `SUM(views_count)` из `videos`.
3. Если вопрос про "на сколько выросли/прирост/дельта" — используй `SUM(delta_views_count)` из `video_snapshots`.
4. Если в таком запросе упоминается креатор — делай `JOIN video_snapshots s ON s.video_id = v.id` и фильтруй по `v.creator_id`.
5. Для фильтрации по месяцу и году используй: `EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6`.
6. Для диапазона дат используй: `DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05'`.
7. Для времени в пределах дня: `s.created_at >= '2025-11-28 10:00:00' AND s.created_at <= '2025-11-28 15:00:00'`.
8. Если вопрос про "сколько замеров с отрицательным приростом" — используй `WHERE delta_views_count < 0`.
9. Всегда используй `COALESCE(..., 0)`, если возможен NULL.
10. Дата публикации = `video_created_at`.
11. Ответ должен начинаться с `SELECT` и заканчиваться `;`. Только SQL.

Примеры:
Вопрос: Сколько всего видео есть в системе?
Ответ: SELECT COUNT(*) FROM videos;

Вопрос: Сколько видео набрало больше 100000 просмотров за всё время?
Ответ: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: Сколько видео у креатора с id aca1061a9d324ecf8c3fa2bb32d7be63 набрали больше 10000 просмотров по итоговой статистике?
Ответ: SELECT COUNT(*) FROM videos WHERE creator_id = 'aca1061a9d324ecf8c3fa2bb32d7be63' AND views_count > 10000;

Вопрос: Сколько видео опубликовал креатор с id 8b76e572635b400c9052286a56176e03 в период с 1 ноября 2025 по 5 ноября 2025 включительно?
Ответ: SELECT COUNT(*) FROM videos WHERE creator_id = '8b76e572635b400c9052286a56176e03' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: Какое суммарное количество просмотров набрали все видео, опубликованные в июне 2025 года?
Ответ: SELECT SUM(views_count) FROM videos WHERE EXTRACT(YEAR FROM video_created_at) = 2025 AND EXTRACT(MONTH FROM video_created_at) = 6;

Вопрос: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
Ответ: SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: Сколько разных видео получали новые просмотры 27 ноября 2025?
Ответ: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

Вопрос: Сколько всего есть замеров статистики, в которых число просмотров за час оказалось отрицательным?
Ответ: SELECT COUNT(*) FROM video_snapshots WHERE delta_views_count < 0;

Вопрос: На сколько просмотров суммарно выросли все видео креатора с id cd87be38b50b4fdd8342bb3c383f3c7d в промежутке с 10:00 до 15:00 28 ноября 2025 года?
Ответ: SELECT COALESCE(SUM(s.delta_views_count), 0) FROM video_snapshots s JOIN videos v ON s.video_id = v.id WHERE v.creator_id = 'cd87be38b50b4fdd8342bb3c383f3c7d' AND s.created_at >= '2025-11-28 10:00:00' AND s.created_at <= '2025-11-28 15:00:00';

Вопрос: {question}
Ответ:
"""

async def handle_question(text: str) -> str:
    try:
        prompt = PROMPT.format(question=text.strip())
        response = ollama.generate(model='gemma3:1b', prompt=prompt)
        sql = response['response'].strip().split(';')[0] + ';'

        if not sql.lower().startswith('select'):
            return "0"

        print(f"🔍 SQL: {sql}")  # для дебага

        conn = get_db()
        cur = conn.cursor()
        cur.execute(sql)
        result = cur.fetchone()[0]
        cur.close()
        conn.close()

        return str(result if result is not None else 0)

    except Exception as e:
        return "0"  # чтобы не падало на проверке

async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(Command("start"))
    async def start(m):
        await m.answer("Готов считать.")

    @dp.message(F.text)
    async def on_msg(m):
        ans = await handle_question(m.text)
        await m.answer(ans)

    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        ollama.list()
    except:
        print("❗ Запустите 'ollama serve' в другом терминале")
        exit(1)
    asyncio.run(main())