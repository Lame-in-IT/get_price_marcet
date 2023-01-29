from Parser_OZON_data_bd import record_bd_OZON
from Parser_WB_data_bd import record_bd_WB
import schedule

def run_price():
    record_bd_OZON()
    run_price_1()

def run_price_1():
    record_bd_WB()
    
def main():
    schedule.every().day.at("09:00").do(run_price)
    while True:
        schedule.run_pending()

if __name__ == '__main__':
    main()