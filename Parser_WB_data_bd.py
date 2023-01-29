import requests
import psycopg2
from datetime import datetime
from datetime import date, timedelta
from config_WB import headers_OZON
from dict_art_WB import dict_art_WB
from conect_bd import *
import time

corrent_date = "{:%Y-%m-%d}".format(datetime.now())
corrent_date_1 = "{:%Y, %m, %d}".format(datetime.now())
format_date = corrent_date_1.split(",")
year = int(format_date[0])
month = int(format_date[1])
day = int(format_date[2])
first_date = date(year, month, day)
duration = timedelta(days=1)
list_last_day = []
for d in range(duration.days + 1):
    day = first_date - timedelta(days=d)
    list_last_day.append(day)
corrent_date_last = list_last_day[1]
attempt = 0

def constat():
    global attempt
    attempt += 1

def get_history_price_WB_base():
    try:
        rinquiry = requests.get(url='https://suppliers-api.wildberries.ru/public/api/v1/info?quantity=1', headers=headers_OZON).json()
        list_artikle_WB = []
        list_name_WB = []
        list_new_price_WB = []
        list_link_WB = []
        for item_WB in dict_art_WB:
            url_WB = f"https://www.wildberries.ru/catalog/{item_WB}/detail.aspx"
            list_artikle_WB.append(dict_art_WB[item_WB][0])
            list_name_WB.append(dict_art_WB[item_WB][1])
            list_link_WB.append(url_WB)
            time_list_art = []
            for item_price in rinquiry:
                art = str(item_price["nmId"])
                time_list_art.append(art)
                if art in str(item_WB):
                    discount_OZON = 100 - item_price["discount"]
                    now_price_WB = int(item_price["price"] /100 * discount_OZON)
                    list_new_price_WB.append(now_price_WB)
            if str(item_WB) not in time_list_art:
                list_new_price_WB.append(0)
        return [list_artikle_WB, list_name_WB, list_new_price_WB, list_link_WB]
    except Exception as e:
        print(e)
        print("Тайм айут от сервера.")
        time.sleep(30)
        if attempt <= 5:
            print("Перезапуск (get_history_price_WB_base)")
            constat()
            get_history_price_WB_base()

def record_bd_WB():
    data_price_WB = get_history_price_WB_base()
    list_artikle_WB = data_price_WB[0]
    list_name_WB = data_price_WB[1]
    list_new_price_WB = data_price_WB[2]
    list_link_WB = data_price_WB[3]
    try:
        connection = psycopg2.connect(
            database=database,
            user=user,
            password=password,
            host=host,
            port=port
        )
        connection.autocommit = True
        with connection.cursor() as cursor:
            cursor.execute(
                f"""SELECT EXISTS (SELECT * FROM price_MARKET_WB WHERE Дата_проверки = '{corrent_date}');"""
            )
            list_curs = cursor.fetchall()
        if list_curs[0][0] == False:
            list_old_pr_WB = []
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""SELECT Новая_цена from price_MARKET_WB WHERE Дата_проверки = '{corrent_date_last}';"""
                )
                list_Старая_цена = cursor.fetchall()
                for iten_Старая_цена in list_Старая_цена:
                    for item_Старая_цена in iten_Старая_цена:
                        list_old_pr_WB.append(item_Старая_цена)
            for index_art, item_art in enumerate(list_artikle_WB):
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO price_MARKET_WB(Артикул, Название_товара, Старая_цена, Новая_цена, Дата_проверки, 
                                    Ссылка_на_товар) VALUES(%s, %s, %s, %s, %s, %s)""",
                                   [item_art, list_name_WB[index_art], list_old_pr_WB[index_art],
                                    list_new_price_WB[index_art], corrent_date,
                                    list_link_WB[index_art]])
            print("Запись цен WB добавлена")
        elif list_curs[0][0] == True:
            list_old_pri_WB = []
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""SELECT Новая_цена from price_MARKET_WB WHERE Дата_проверки = '{corrent_date}';"""
                )
                list_pr = cursor.fetchall()
                for iten_Новая_цена in list_pr:
                    for item_Новая_цена_1 in iten_Новая_цена:
                        list_old_pri_WB.append(item_Новая_цена_1)
            for index_pric_1, item_pric_1 in enumerate(list_old_pri_WB):
                with connection.cursor() as cursor:
                    cursor.execute("""UPDATE price_MARKET_WB SET Старая_цена = %s WHERE Дата_проверки = %s AND Артикул = %s""", [
                        item_pric_1, corrent_date, list_artikle_WB[index_pric_1]])
                with connection.cursor() as cursor:
                    cursor.execute("""UPDATE price_MARKET_WB SET Старая_цена = %s WHERE Дата_проверки = %s AND Артикул = %s""", [
                        list_new_price_WB[index_pric_1], corrent_date, list_artikle_WB[index_pric_1]])
            print("Запись цен WB обновлена")
    except Exception as ex:
        time.sleep(10)
        print(ex)
        if attempt <= 5:
            print("Перезапуск (record_bd_WB)")
            constat()
            record_bd_WB()
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    record_bd_WB()
