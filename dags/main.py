from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.python_operator import PythonOperator
# from airflow.operators.bash import BashOperator
import telebot
import pandas as pd
import requests
import ast
from datetime import datetime
bot = telebot.TeleBot(":)")


@bot.message_handler(commands=['start'])
def start_friendship(message):
    cid = message.chat.id
    df = pd.read_csv("chat_ids.csv", index_col=0)
    if cid not in df["chat_id"].values:
        df.loc[df.index[-1]+1] = cid
        df.to_csv("chat_ids.csv")
        bot.send_message(cid, f"Ты записан в список")
        bot.send_message(cid, f"Получай каждый день фразу дня и развивайся!")
    else:
        bot.send_message(cid, "Ты уже есть в списке")
    #  понять, что человек вышел из бота


def get_phrase_day():
    URL = "https://translate.yandex.ru/?lang=en-ru"
    webpage = requests.get(URL)
    web_rus = webpage.content.decode("utf-8")
    index_start_daily = web_rus.find("DAILY_PHRASES")
    after_part = web_rus[index_start_daily + len("DAILY_PHRASES") + 2:]
    string_list = after_part[:after_part.find("]") + 1]
    # print(index_start_daily)
    # print(web_rus[index_start_daily-30:index_start_daily+20])
    # print(after_part)
    # print(string_list)

    daily_phrases = ast.literal_eval(string_list)

    # [{'text': 'have a yellow belly',
    #   'details': 'иметь желтый живот',
    #   'translation': 'быть трусливым',
    #   'lang': 'en'},
    #  {'text': 'ahead of the pack', 'translation': 'впереди всех', 'lang': 'en'}]
    daily_dict = dict()
    for num,element in enumerate(daily_phrases):
        daily_dict[f"phr{num+1}_en"] = element["text"]
        daily_dict[f"phr{num+1}_ru"] = element["translation"]
    daily_dict["phr_count"] = len(daily_phrases)
    daily_dict["date"] = pd.to_datetime(datetime.now())
    return daily_dict


def append_phrases_csv(daily_dict):
    actual_day = pd.to_datetime(datetime.now()).day
    df = pd.read_csv("day_phrases.csv", index_col=0)
    for date in df["date"][::-1]:
        if date.day == actual_day:
            break
    else:
        df.append(daily_dict, ignore_index=True)
        df.to_csv("day_phrases.csv")
        return "DataFrame updated"
    return "Today phrases already exists"


def sending_day_phrase():
    actual_day = pd.to_datetime(datetime.now()).day
    df_id = pd.read_csv("chat_ids.csv", index_col=0)
    df_phrases = pd.read_csv("day_phrases.csv", index_col=0)
    for i in df_phrases.index[::-1]:
        if df_phrases["date"][i].day == actual_day:
            actual_index = i
            break
    else:
        return "nothing to send"
    series_row = df_phrases.loc[actual_index]
    if series_row.isna().sum()!=0:
        phrases = {series_row["phr1_en"]:series_row["phr1_ru"]}
    else:
        phrases = {series_row["phr1_en"]:series_row["phr1_ru"], series_row["phr2_en"]:series_row["phr2_ru"]}

    for id in df_id["chat_id"].values:
        bot.send_message(id, f"Салам! Количество фраз дня: {int(df_phrases.loc[actual_index]['phr_count'])} ")
        for word_eng,word_rus in phrases.items():
            bot.send_message(id, f"{word_eng} - {word_rus}")
        # bot.send_message(id, f"{day_phrase_eng} - {day_phrase_rus}")
    # return "All is amazing!"


# functions for dag

def append_phrases():
    daily_dict = get_phrase_day()
    result = append_phrases_csv(daily_dict)
    return result


def send_phrase_messages():
    sending_day_phrase()



with DAG(
    dag_id="telegram_bot_phrase_day",
    schedule_interval="@daily",
) as dag:
    send_phrase_messages = PythonOperator(
        task_id = "send_phrase_messages",
        python_callable= send_phrase_messages,
        start_date= datetime(2021,1,1)
    )
    append_phrases = PythonOperator(
        task_id="append_phrases",
        python_callable=append_phrases,
        start_date=datetime(2021, 1, 1)
    )

bot.infinity_polling()

# @bot.message_handler(commands=['gethuy'])
# def start_friendship(message):
#     cid = message.chat.id
#     sending_day_phrase()
