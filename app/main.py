import pandas as pd
import re
from io import BytesIO
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from loguru import logger
from pathlib import Path
import asyncpg


# Получаем абсолютный путь к папке logs на уровне проекта
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_DIR = BASE_DIR / "logs"


# Убедимся, что папка logs существует
LOG_DIR.mkdir(exist_ok=True)


# Настройка Loguru для логирования с ротацией
logger.add(
    LOG_DIR / "{time:YYYY-MM-DD}.log",
    rotation="3 weeks",
    retention="3 weeks",
    compression="zip"
)

# Загрузка переменных окружения
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")


# Загрузка таблицы с ID авторов
author_ids = pd.read_csv("author_ids.csv")  # Таблица Author, TelegramID
author_dict = dict(zip(author_ids["Author"], author_ids["TelegramID"]))

BOSS_ID = author_dict.get("Пашковский Денис Юзэфович")
if not BOSS_ID:
    logger.error("ID начальника не найден в author_dict")


# Настраиваем подключение к базе данных
async def get_db_pool():
    return await asyncpg.create_pool(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )


async def insert_into_orders(pool, orders):
    query = """
    INSERT INTO orders (
        payment_date,
        payment_number,
        payment_amount,
        account_number,
        contractor_name,
        manager_name,
        order_status,
        highlight_color
    )
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
    """
    async with pool.acquire() as connection:
        for order in orders:
            await connection.execute(query, *order)


# Удаляем из первой таблицы всю ненужную шелуху вокруг номера счета
def extract_account_number_1(text: str) -> str:
    pattern = r"(счет|сч|сч\.|№|No|N|по\s+счету|на\s+оплату|счету)\s*(№|N)?\s*(\d{1,5})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        # Проверяем, что group(3) содержит значение
        number = match.group(3)
        if number and number.isdigit():  # Убедимся, что это цифры
            number = int(number)
            if 1 <= number <= 20000:
                return str(number)
    return None


# Удаляем из второй таблицы всю ненужную шелуху вокруг номера счета
def extract_account_number_2(text: str) -> str:
    match = re.search(r"0(\d+)$", text)
    if match:
        number = match.group(1)
        if number and number.isdigit():  # Проверяем, что это цифры
            number = int(number)
            if 1 <= number <= 20000:
                return str(number)
    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()  # Очистка состояния
    await update.message.reply_text("Пришлите первую таблицу (XLS или HTML).")


async def process_tables(pool, table1: pd.DataFrame, table2: pd.DataFrame):
    messages = []
    unmatched = []
    orders = []

    # Фильтруем строки, где колонка "Кредит" пуста (Null/NaN)
    table1 = table1.dropna(subset=["Кредит"]).copy()

    # Обработка первой таблицы
    table1["Extracted"] = table1["Назначение"].apply(extract_account_number_1)

    # Обработка второй таблицы
    table2["Extracted"] = table2["Номер"].apply(extract_account_number_2)

    # Сопоставление счетов
    for _, row in table1.iterrows():
        account_number = row["Extracted"]
        if account_number:
            match = table2[table2["Extracted"] == account_number]
            if not match.empty:
                author = match.iloc[0]["Автор"]
                client = match.iloc[0]["Клиент"]
                credit = row["Кредит"]
                purpose = row["Назначение"]
                payment_date = row["Дата"]
                payment_number = row["Док."]
                contractor_name = client
                manager_name = author

                # Проверяем, существует ли уже запись с такими payment_date, payment_number и contractor_name
                async with pool.acquire() as conn:
                    async with conn.transaction():
                        result = await conn.fetch(
                            """
                            SELECT 1 
                            FROM orders 
                            WHERE payment_date = $1 AND payment_number = $2 AND contractor_name = $3
                            """,
                            payment_date, payment_number, contractor_name
                        )
                        if result:
                            continue  # Запись уже существует, пропускаем её

                        # Подготовка строки для таблицы orders
                        orders.append((
                            payment_date,
                            payment_number,
                            credit,
                            account_number,
                            contractor_name,
                            manager_name,
                            'Заказ оплачен',  # Статус по умолчанию
                            'red'  # Цвет по умолчанию
                        ))

                        messages.append(
                            (author, f"Клиент {client} оплатил сумму 💲{credit}💲\n- {purpose}"))
            else:
                unmatched.append(row["Назначение"])
        else:
            unmatched.append(row["Назначение"])

    # Запись данных в таблицу orders
    if orders:
        await insert_into_orders(pool, orders)

    logger.info(f"Messages: {messages}")
    logger.info(f"Unmatched: {unmatched}")

    return messages, unmatched


async def handle_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    document = update.message.document

    if not document:
        await update.message.reply_text("Это не файл. Попробуйте снова.")
        return

    # Сохраняем user_id, если это первая таблица
    if "uploader_id" not in context.user_data:
        context.user_data["uploader_id"] = user_id

    # Проверяем название файла и его расширение
    file_name = document.file_name.lower()
    if not (file_name.endswith(".xls") or file_name.endswith(".xlsx") or file_name.endswith(".html") or file_name.endswith(".htm")):
        await update.message.reply_text("Формат Вашего сообщения не соответствует запросу, прошу ещё раз пришлите таблицу.")
        return

    # Загружаем файл
    file = await context.bot.get_file(document.file_id)
    file_bytes = BytesIO(await file.download_as_bytearray())

    pool = await get_db_pool()

    try:
        if "table1" not in context.user_data:
            # Загружаем первую таблицу
            try:
                if file_name.endswith(".html") or file_name.endswith(".htm"):
                    table1 = pd.read_html(file_bytes)[0]
                else:
                    table1 = pd.read_excel(file_bytes)

                context.user_data["table1"] = table1
                await update.message.reply_text("Первая таблица сохранена. Пришлите вторую таблицу.")
            except Exception as e:
                await update.message.reply_text(f"Ошибка при загрузке первой таблицы: {e}")
                return

        elif "table2" not in context.user_data:
            # Загружаем вторую таблицу
            try:
                if file_name.endswith(".html") or file_name.endswith(".htm"):
                    table2 = pd.read_html(file_bytes)[0]
                else:
                    table2 = pd.read_excel(file_bytes)

                context.user_data["table2"] = table2

                # Обрабатываем таблицы
                table1 = context.user_data["table1"]
                table2 = context.user_data["table2"]

                messages, unmatched = await process_tables(pool, table1, table2)

                # Отправляем сообщения авторам
                for author, message_text in messages:
                    tg_id = author_dict.get(author, author_dict["Unknown"])
                    try:
                        await context.bot.send_message(chat_id=tg_id, text=message_text)
                        logger.info(f"Сообщение отправлено автору {author}: {message_text}")  # noqa
                    except Exception as e:
                        logger.error(
                            f"Ошибка при отправке сообщения автору {author}: {e}")

                # Подготовка сообщений для начальника
                boss_messages = []
                for author, message_text in messages:
                    boss_messages.append(f"Сообщение отправлено автору {author}: {message_text}")  # noqa
                if unmatched:
                    boss_messages.append("🚨 НЕ РАСПОЗНАННЫЕ ОПЛАТЫ 🚨")
                    boss_messages.extend(unmatched)

                # Отправляем сообщения начальнику
                if boss_messages:
                    try:
                        await context.bot.send_message(
                            chat_id=BOSS_ID,
                            text="\n\n".join(boss_messages)
                        )
                        logger.info("Сообщения успешно отправлены начальнику.")
                    except Exception as e:
                        logger.error(
                            f"Ошибка при отправке сообщений начальнику: {e}")

                # Отправляем сообщения для нераспознанных оплат
                if unmatched:
                    unmatched_id = context.user_data["uploader_id"]
                    unwanted_keywords = [
                        "сальдо", "итог оборотов", "дебет", "кредит"]

                    filtered_rows = table1[
                        table1["Назначение"].isin(unmatched) &
                        ~table1["Назначение"].str.contains(
                            "|".join(unwanted_keywords), case=False)
                    ]

                    unmatched_messages = [
                        f"Клиент {row['Контрагент']} оплатил сумму 💲{row['Кредит']}💲\n- {row['Назначение']}"  # noqa
                        for _, row in filtered_rows.iterrows()
                    ]

                    if unmatched_messages:
                        unmatched_messages.insert(
                            0, "🚨 НЕ РАСПОЗНАННЫЕ ОПЛАТЫ 🚨")
                        await context.bot.send_message(chat_id=unmatched_id, text="\n\n".join(unmatched_messages))
                        logger.info(f"Неотсортированные данные отправлены пользователю {unmatched_id}")  # noqa

                context.user_data.clear()
                await update.message.reply_text("‼️ Обработка завершена. Все данные сохранены в базу ‼️")
            except Exception as e:
                await update.message.reply_text(f"Ошибка при загрузке второй таблицы: {e}")
                return
        else:
            await update.message.reply_text("Вы уже отправили обе таблицы. Введите /start, чтобы начать заново.")
    finally:
        await pool.close()


def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(
        filters.Document.ALL, handle_document))
    application.run_polling()


if __name__ == "__main__":
    main()
