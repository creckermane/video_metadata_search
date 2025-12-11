import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
import psycopg2
import ollama

# Настройка логов
logging.basicConfig(level=logging.INFO)

# Токен от BotFather
BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"


# Подключение к БД
def get_db_conn():
    return psycopg2.connect(
        host="localhost",
        database="video_stats",
        user="user",
        password="password"
    )


# Промт для Ollama
SYSTEM_PROMPT = """
Ты — SQL-ассистент для аналитики видео. Есть две таблицы:

1.  **videos**: id, creator_id, video_created_at, views_count, likes_count, comments_count, reports_count, created_at, updated_at.
2.  **video_snapshots**: id, video_id, views_count, likes_count, comments_count, reports_count, delta_views_count, delta_likes_count, delta_comments_count, delta_reports_count, created_at, updated_at.

Правила:
- Пользователь спрашивает на русском.
- Ты возвращаешь ТОЛЬКО один SQL-запрос, который вернет одно число.
- Не используй SELECT *, только нужные поля.
- Используй агрегатные функции (COUNT, SUM и т.д.) если нужно.
- Для прироста за день — суммируй delta_* из video_snapshots за этот день.
- Для количества видео с условием — используй COUNT(*) из videos.
- Время в базе в UTC. Дата '28 ноября 2025' = '2025-11-28'.
- Ответ ДОЛЖЕН начинаться с SELECT и заканчиваться ;. Только SQL, никаких слов.

Пример:
Вопрос: "На сколько просмотров в сумме выросли все видео 28 ноября 2025?"
Ответ: SELECT SUM(delta_views_count) FROM video_snapshots WHERE DATE(created_at) = '2025-11-28';

Вопрос: {question}
Ответ:
"""


# Обработка запроса через LLM
async def process_natural_language(query: str) -> str:
    try:
        # Формируем промт
        prompt = SYSTEM_PROMPT.format(question=query)

        # Запрос к локальной модели
        response = ollama.generate(model='phi3', prompt=prompt)
        sql_query = response['response'].strip()

        # Простая проверка
        if not sql_query.lower().startswith('select'):
            return "Не удалось распознать запрос."

        print(f"LLM сгенерировал: {sql_query}")  # Для дебага

        # Выполняем SQL
        conn = get_db_conn()
        cur = conn.cursor()
        cur.execute(sql_query)
        result = cur.fetchone()[0]  # Ожидаем одно число
        cur.close()
        conn.close()

        return str(result or 0)

    except Exception as e:
        return f"Ошибка: {str(e)}"


# Запуск бота
async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()

    @dp.message(Command("start"))
    async def cmd_start(message):
        await message.answer("Привет! Задай вопрос по статистике видео.")

    @dp.message(F.text)
    async def handle_text(message):
        answer = await process_natural_language(message.text)
        await message.answer(answer)

    print("Бот запущен...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    # Проверка Ollama
    try:
        ollama.list()
    except:
        print("Ollama не запущена. Запусти 'ollama serve' в другом окне.")
        exit(1)

    asyncio.run(main())