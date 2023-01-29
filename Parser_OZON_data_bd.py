import requests
import psycopg2
import json
from datetime import datetime
from datetime import date, timedelta
import time
from dict_art_OZON import dict_art_OZON
from config_OZON import headers_OZON
from conect_bd import *

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

def get_price_OZON():
    list_artikle_OZON = []
    list_name_OZON = []
    list_new_price_OZON = []
    list_link_OZON = []
    for item_arlicle in dict_art_OZON:
        try:
            body = {
                "sku": int(item_arlicle)
            }
            rinquiry = requests.post(
                'https://api-seller.ozon.ru/v2/product/info', json=body, headers=headers_OZON).text
            resulte = json.loads(rinquiry)
            # with open("prodaji_OZON_.json", "w", encoding="utf_8") as file_create:
            #     json.dump(resulte, file_create, indent=4, ensure_ascii=False)
            url_OZON = f"https://www.ozon.ru/product/sapogi-dlya-ohoty-nordman-rybalka-{item_arlicle}"
            price_OZON = resulte['result']["marketing_price"].split('.')[0]
            present_image_OZON = resulte['result']["stocks"]["present"]
            if present_image_OZON == 0:
                price_OZON = 0
            list_artikle_OZON.append(dict_art_OZON[item_arlicle][0])
            list_name_OZON.append(dict_art_OZON[item_arlicle][1])
            list_new_price_OZON.append(price_OZON)
            list_link_OZON.append(url_OZON)
        except Exception as ex:
            time.sleep(10)
            print(ex)
            if attempt <= 5:
                print("Перезапуск (get_price_OZON)")
                constat()
                get_price_OZON()
    return [list_artikle_OZON, list_name_OZON, list_new_price_OZON, list_link_OZON]

def record_bd_OZON():
    data_price_OZON = get_price_OZON()
    list_artikle_OZON = data_price_OZON[0]
    list_name_OZON = data_price_OZON[1]
    list_new_price_OZON = data_price_OZON[2]
    list_link_OZON = data_price_OZON[3]
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
                f"""SELECT EXISTS (SELECT * FROM price_MARKET_OZON WHERE Дата_проверки = '{corrent_date}');"""
            )
            list_curs = cursor.fetchall()
        if list_curs[0][0] == False:
            list_old_pr_OZON = []
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""SELECT Новая_цена from price_MARKET_OZON WHERE Дата_проверки = '{corrent_date_last}';"""
                )
                list_Старая_цена = cursor.fetchall()
                for iten_Старая_цена in list_Старая_цена:
                    for item_Старая_цена in iten_Старая_цена:
                        list_old_pr_OZON.append(item_Старая_цена)
            for index_art, item_art in enumerate(list_artikle_OZON):
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO price_MARKET_OZON(Артикул, Название_товара, Старая_цена, Новая_цена, Дата_проверки, 
                                    Ссылка_на_товар) VALUES(%s, %s, %s, %s, %s, %s)""",
                                   [item_art, list_name_OZON[index_art], list_old_pr_OZON[index_art],
                                    list_new_price_OZON[index_art], corrent_date,
                                    list_link_OZON[index_art]])
            print("Запись цен OZON добавлена")
        elif list_curs[0][0] == True:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""DELETE FROM price_MARKET_OZON WHERE Дата_проверки = '{corrent_date}';"""
                )
            list_old_pr_OZON = []
            with connection.cursor() as cursor:
                cursor.execute(
                    f"""SELECT Новая_цена from price_MARKET_OZON WHERE Дата_проверки = '{corrent_date_last}';"""
                )
                list_Старая_цена = cursor.fetchall()
                for iten_Старая_цена in list_Старая_цена:
                    for item_Старая_цена in iten_Старая_цена:
                        list_old_pr_OZON.append(item_Старая_цена)
            for index_art, item_art in enumerate(list_artikle_OZON):
                with connection.cursor() as cursor:
                    cursor.execute("""INSERT INTO price_MARKET_OZON(Артикул, Название_товара, Старая_цена, Новая_цена, Дата_проверки, 
                                    Ссылка_на_товар) VALUES(%s, %s, %s, %s, %s, %s)""",
                                   [item_art, list_name_OZON[index_art], list_old_pr_OZON[index_art],
                                    list_new_price_OZON[index_art], corrent_date,
                                    list_link_OZON[index_art]])
            print("Запись цен OZON обновлена")
    except Exception as ex:
        time.sleep(10)
        print(ex)
    finally:
        if connection:
            connection.close()

if __name__ == '__main__':
    record_bd_OZON()
