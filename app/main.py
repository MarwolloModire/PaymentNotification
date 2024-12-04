import pandas as pd
import re
from io import BytesIO
import os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from loguru import logger
from pathlib import Path

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


# Удаляем из первой таблицы всю ненужную шелуху вокруг номера счета
def extract_account_number_1(text: str) -> str:
    pattern = r"(счет|сч|сч\.|№|No|N|по\s+счету|на\s+оплату)?\s*№?\s*(\d{1,5})"
    match = re.search(pattern, text, re.IGNORECASE)
    if match:
        number = int(match.group(2))
        if 1 <= number <= 20000:
            return str(number)
    return None


# Удаляем из второй таблицы всю ненужную шелуху вокруг номера счета
def extract_account_number_2(text: str) -> str:
    match = re.search(r"0(\d+)$", text)
    if match:
        number = int(match.group(1))
        if 1 <= number <= 20000:
            return str(number)
    return None


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Пришлите первую таблицу (XLS или HTML).")


def process_tables(table1: pd.DataFrame, table2: pd.DataFrame):
    messages = []
    unmatched = []

    # Фильтруем строки, где колонка "Кредит" пуста (Null/NaN)
    table1 = table1.dropna(subset=["Кредит"])

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
                messages.append((author, f"Клиент {client} оплатил сумму 💲{credit}💲\n- {purpose}"))  # noqa
            else:
                unmatched.append(row["Назначение"])
        else:
            unmatched.append(row["Назначение"])

    # Логирование результатов
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

    # Получаем название файла и проверяем расширение
    file_name = document.file_name.lower()
    if not (file_name.endswith(".xls") or file_name.endswith(".xlsx") or file_name.endswith(".html") or file_name.endswith(".htm")):
        await update.message.reply_text("Формат Вашего сообщения не соответствует запросу, прошу ещё раз пришлите таблицу.")
        return

    # Скачиваем файл
    file = await context.bot.get_file(document.file_id)
    file_bytes = BytesIO(await file.download_as_bytearray())

    # Обработка файла в зависимости от формата
    if "table1" not in context.user_data:
        try:
            if file_name.endswith(".html") or file_name.endswith(".htm"):
                table1 = pd.read_html(file_bytes)[0]
                context.user_data["table1"] = table1
                await update.message.reply_text("Первая таблица сохранена. Пришлите вторую таблицу.")
            else:
                context.user_data["table1"] = pd.read_excel(file_bytes)
                await update.message.reply_text("Первая таблица сохранена. Пришлите вторую таблицу.")
        except Exception as e:
            await update.message.reply_text(f"Ошибка при загрузке таблицы: {e}")
            return
    elif "table2" not in context.user_data:
        try:
            if file_name.endswith(".html") or file_name.endswith(".htm"):
                table2 = pd.read_html(file_bytes)[0]
                context.user_data["table2"] = table2
            else:
                context.user_data["table2"] = pd.read_excel(file_bytes)

            table1 = context.user_data["table1"]
            table2 = context.user_data["table2"]

            logger.info(f"Table 1: {table1.head()}")
            logger.info(f"Table 2: {table2.head()}")

            messages, unmatched = process_tables(table1, table2)

            for author, message_text in messages:
                tg_id = author_dict.get(author, author_dict["Unknown"])
                try:
                    await context.bot.send_message(chat_id=tg_id, text=message_text)
                    logger.info(f"Сообщение отправлено автору {author}: {message_text}")  # noqa
                except Exception as e:
                    logger.error(
                        f"Ошибка при отправке сообщения автору {author}: {e}")

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

                # Добавляем заголовок для нераспознанных оплат
                if unmatched_messages:
                    unmatched_messages.insert(0, "🚨 НЕ РАСПОЗНАННЫЕ ОПЛАТЫ 🚨")

                    # Отправляем нераспознанные данные пользователю, который загрузил таблицы
                    await context.bot.send_message(chat_id=unmatched_id, text="\n\n".join(unmatched_messages))
                    logger.info(f"Неотсортированные данные отправлены пользователю {unmatched_id}")  # noqa

            context.user_data.clear()
            await update.message.reply_text("‼️ Обработка завершена. Все сообщения отправлены ‼️")

        except Exception as e:
            await update.message.reply_text(f"Ошибка при загрузке второй таблицы: {e}")
            return
    else:
        await update.message.reply_text("Вы уже отправили обе таблицы. Введите /start, чтобы начать заново.")


def main():
    application = Application.builder().token(BOT_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(
        filters.Document.ALL, handle_document))
    application.run_polling()


if __name__ == "__main__":
    main()
