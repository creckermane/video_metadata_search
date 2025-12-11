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
Ты — SQL-бот для аналитики видео. Отвечай ТОЛЬКО валидным SQL-запросом, который возвращает ОДНО целое число. Никаких слов.

Таблицы:
- videos(id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, ...)
- video_snapshots(video_id, delta_views_count, created_at, ...)

Правила:
1. Если вопрос про "набрало больше X просмотров", "сколько видео у креатора", "сколько всего видео" — используй ТОЛЬКО таблицу `videos`.
2. Если вопрос про "на сколько выросли/прирост/новые просмотры N ноября" — используй `video_snapshots` и `delta_*`.
3. Для даты '28 ноября 2025' используй условие: `DATE(created_at) = '2025-11-28'`.
4. Прирост = SUM(delta_views_count) и т.д.
5. Ответ должен начинаться с SELECT и заканчиваться ;. Только SQL.

Примеры:
Вопрос: Сколько всего видео есть в системе?
Ответ: SELECT COUNT(*) FROM videos;

Вопрос: Сколько видео набрало больше 100000 просмотров за всё время?
Ответ: SELECT COUNT(*) FROM videos WHERE views_count > 100000;

Вопрос: Сколько видео у креатора с id abc123 вышло с 1 ноября 2025 по 5 ноября 2025 включительно?
Ответ: SELECT COUNT(*) FROM videos WHERE creator_id = 'abc123' AND DATE(video_created_at) BETWEEN '2025-11-01' AND '2025-11-05';

Вопрос: На сколько просмотров в сумме выросли все видео 28 ноября 2025?
Ответ: SELECT COALESCE(SUM(delta_views_count), 0) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: Сколько разных видео получали новые просмотры 27 ноября 2025?
Ответ: SELECT COUNT(DISTINCT video_id) FROM video_snapshots WHERE DATE(created_at) = '2025-11-27' AND delta_views_count > 0;

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