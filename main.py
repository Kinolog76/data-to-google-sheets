from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.proxy import Proxy, ProxyType
from datetime import timedelta, date, datetime
import time
import json
import shutil
import pandas as pd
import os
import re
import random
import telegram
import asyncio
import pickle
import gspread
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import logging

logging.basicConfig(
    filename='./script.log', 
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)

#! Настройка доступа к Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file('./accesses/credentials.json', scopes=scope)
client = gspread.authorize(creds)

#! Открытие Google Таблицы
spreadsheet = client.open_by_key('GOOGLE_SHEET_ID')

all_data = {}
aff_lucky_url = 'SIGN_IN_URL'

TOKEN = 'TELEGRAM_TOKEN'
CHAT_ID = 'TELEGRAM_CHAT_ID'


async def send_telegram_message(message):
    try:
        bot = telegram.Bot(token=TOKEN)
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        logging.error(f"Ошибка отправки сообщения в Telegram: {str(e)}")

    
# ! функции
def get_current_selector(f_type):
    """
    Находит все поля c выпадающим меню, затем ищет нужное по ключу в переменной type
    """
    add_selects = driver.find_elements(By.CLASS_NAME, 'ant-select-selector')
    for add_select in add_selects:
        if add_select.text == f_type:
            return add_select

def set_current_date(end_date, s_field, e_field):
    """Получает дату в формате гггг-мм-дд, затем высчитывает дату неделю назад и передает
    полученные значения в поля для начальной и конечной даты, предварительно очистив их
    """
    one_week = timedelta(weeks=1)
    start_date = end_date - one_week

    s_field.click()
    driver.execute_script('arguments[0].value = "";', s_field)
    s_field.send_keys(str(start_date))
    e_field.click()
    driver.execute_script('arguments[0].value = "";', e_field)
    e_field.send_keys(str(end_date))

def parse_data():
    '''Получает все данные из таблицы и записывает их в словарь по указанным ключам
    '''
    all_data = list()
    table_items = driver.find_elements(By.CSS_SELECTOR, '.sc-sPYgB tbody tr')
    for table_item in table_items:
        table_data = table_item.find_elements(By.CSS_SELECTOR, 'td')
        item_as_object = dict()
        item_as_object['Subakk'] = table_data[0].text
        item_as_object['Hosts'] = table_data[1].text
        item_as_object['Clicks'] = table_data[2].text
        item_as_object['Impression'] = table_data[3].text
        item_as_object['All'] = table_data[4].text
        item_as_object['Approved'] = table_data[5].text
        item_as_object['Pending'] = table_data[6].text
        item_as_object['Hold'] = table_data[7].text
        item_as_object['Declined'] = table_data[8].text
        all_data.append(item_as_object)
    return all_data

#! Функция для добавления данных в гугл таблицу
def add_data_to_sheet(data, worksheet):
    logging.info('Добавление данных в Google Sheet')
    row_index = 3

    for section_name, section_data in data.items():
        if not isinstance(section_data, list):
            continue

        # Определяем цвет фона заголовка в зависимости от наличия данных
        section_color = '#90EE90' if section_data else '#FF6347'

        # Добавление заголовка секции
        cell_range = f'A{row_index}:I{row_index}'
        worksheet.merge_cells(cell_range)
        worksheet.update_cell(row_index, 1, section_name)
        worksheet.format(cell_range, {
            'backgroundColor': {
                'red': int(section_color[1:3], 16) / 255,
                'green': int(section_color[3:5], 16) / 255,
                'blue': int(section_color[5:7], 16) / 255
            },
            'horizontalAlignment': 'CENTER',
            'verticalAlignment': 'MIDDLE',
            'textFormat': {'bold': True, 'fontSize': 16}
        })
        row_index += 1
        time.sleep(1.5)  # Задержка между запросами для избежания ошибки

        # Добавление headers под заголовком секции
        header_row_index = row_index
        worksheet.append_row(headers)
        worksheet.format(f'A{header_row_index}:I{header_row_index}', {
            'backgroundColor': {'red': 1.0, 'green': 1.0, 'blue': 0.0}
        })
        row_index += 1
        time.sleep(1.5)  # Задержка между запросами для избежания ошибки

        if section_data:
            for entry in section_data:
                row = [entry.get(header, "") for header in headers]
                worksheet.append_row(row)
                row_index += 1
                time.sleep(1.5)  # Задержка между запросами для избежания ошибки

        # Добавление ряда отступа между таблицами
        row_index += 3

#! Функция для добавления данных в локальную таблицу
def generate_excel_table(data, file_name):
    logging.info(f'Генерация Excel таблицы: {file_name}')
    '''
    Генерирует таблицу Excel
    Добавляет данные с .json в таблицу
    Сохраняет ее в указанное место
    '''
    with pd.ExcelWriter(file_name, engine='xlsxwriter') as writer:
        # Создаем один лист для всех данных
        sheet_name = 'Data'
        start_row = 0
        
        # Формат для заголовка
        date_format = writer.book.add_format({
            'align': 'center', 
            'valign': 'vcenter', 
            'bg_color': '#ADD8E6',
            'bold': True,
            'font_size': 16,
        })
        header_format = writer.book.add_format({
            'bold': True,
            'bg_color': '#f8ff00',
            'align': 'center', 
            'valign': 'vcenter', 
        })
        number_format = writer.book.add_format({
            'align': 'center', 
            'valign': 'vcenter',
            'num_format': '#,##0'  # Формат чисел с запятыми
        })
        text_format = writer.book.add_format({
            'valign': 'vcenter',
        })
        parse_date_format = writer.book.add_format({
            'valign': 'vcenter',
            'align': 'center',
            'bold': True,
            'bg_color': '#ADD8E6',  # Синий цвет фона
        })
        
        # Получение объекта рабочего листа
        worksheet = writer.sheets.get(sheet_name) or writer.book.add_worksheet(sheet_name)
        
        current_date = datetime.now().strftime('%Y-%m-%d')
        worksheet.merge_range(start_row, 0, start_row, 9, current_date, date_format)
        start_row += 2
        
        for section_name, section_data in data.items():
            # Пропускаем ключи, которые не являются списками
            if not isinstance(section_data, list):
                continue
            
            #! Определяем цвет фона заголовка в зависимости от наличия данных
            if section_data:
                section_format = writer.book.add_format({
                    'align': 'center', 
                    'valign': 'vcenter', 
                    'bg_color': '#90EE90',  # Зеленый фон
                    'bold': True,
                    'font_size': 16,
                })
            else:
                section_format = writer.book.add_format({
                    'align': 'center', 
                    'valign': 'vcenter', 
                    'bg_color': '#FF6347',  # Красный фон
                    'bold': True,
                    'font_size': 16,
                })
            
            headers = list(section_data[0].keys()) if section_data else []
            
            worksheet.merge_range(start_row, 0, start_row, 9, section_name, section_format)
            start_row += 1
            
            if section_data:
                df = pd.DataFrame(section_data, columns=headers)
                
                # Запись заголовков колонок
                worksheet.write(start_row, 0, 'Sprint', header_format)
                for col_num, value in enumerate(headers, start=1):
                    worksheet.write(start_row, col_num, value, header_format)
                    # Установка ширины колонки по содержимому
                    max_len = max(df[value].astype(str).map(len).max(), len(value)) + 2
                    if value == 'subakk':
                        max_len = max(max_len, 20)
                    worksheet.set_column(col_num, col_num, max_len)
                start_row += 1
                
                # Запись данных в Excel
                for row_num, row_data in df.iterrows():
                    worksheet.write(start_row + row_num, 0, data['parse_date'], parse_date_format)
                    for col_num, value in enumerate(row_data, start=1):
                        if isinstance(value, (int, float)) or value.replace(' ', '').isdigit():
                            worksheet.write_number(start_row + row_num, col_num, float(value.replace(' ', '')), number_format)
                        else:
                            worksheet.write(start_row + row_num, col_num, value, text_format)
                
                # Объединение ячеек для parse_date
                worksheet.merge_range(start_row, 0, start_row + len(df) - 1, 0, data['parse_date'], parse_date_format)
                worksheet.set_column(0, 0, len(data['parse_date']) + 2)
                start_row += len(df)
            
            start_row += 3

# !/ функции

#! Загрузка логинов из файла
with open('./accesses/logins.json', 'r', encoding='utf8') as f:
    logins = json.load(f)

user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.6206.41 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.7891.127 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3161.13 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36 OPR/111.0.0.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.869 YaBrowser/24.1.3.869 (corp) Yowser/2.5 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36',
]

asyncio.run(send_telegram_message(f"Начал работу {datetime.now().strftime('%Y-%m-%d  %H:%M:%S')}"))

for login in logins:
    # ! init
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    agent = random.choice(user_agents)
    options.add_argument(f"user-agent={agent}")
    
    chrome_service = Service('./accesses/chromedriver')
    options.binary_location = "/usr/bin/google-chrome"

    driver = webdriver.Chrome(service=chrome_service, options=options)

    logging.info(f"Открытие страницы: {aff_lucky_url}")
    driver.get(aff_lucky_url)
    
    aff_lucky_login = login['username']
    aff_lucky_password = login['password']
    team_name = login['teamName']
    
    try:
        logging.info("Ожидание элемента с классом 'ant-typography-ellipsis-single-line'")
        WebDriverWait(driver, 60).until(
            EC.visibility_of_element_located((By.CLASS_NAME, "ant-typography-ellipsis-single-line"))
        )
        logging.info("Элемент найден")
    except Exception as e:
        logging.error(f"Ошибка ожидания элемента: {str(e)}")
        driver.quit()
        continue
    
    try:
        # ! login
        WebDriverWait(driver, 30).until(
        ec.visibility_of_element_located((By.XPATH, '//*[@id="root"]/div/main/div/div/div[1]/div/div/form/div[1]/input')))
        time.sleep(random.randint(2, 5))
        username_field = driver.find_element(By.XPATH, '//*[@id="root"]/div/main/div/div/div[1]/div/div/form/div[1]/input')
        for char in aff_lucky_login:
            username_field.send_keys(char)
            time.sleep(0.3)
        time.sleep(random.randint(2, 5))
        driver.find_element(By.XPATH, '//*[@id="root"]/div/main/div/div/div[1]/div/div/form/div[2]/input').send_keys(aff_lucky_password)
        time.sleep(random.randint(2, 5))
        driver.find_element(By.XPATH, '//*[@id="root"]/div/main/div/div/div[1]/div/div/form/button').click()
        time.sleep(random.randint(2, 5))
        WebDriverWait(driver, 30).until(ec.visibility_of_element_located((By.XPATH, '//*[@id="js-scroll-container"]/aside')))
        time.sleep(random.randint(2, 5))
        logging.info(f'Успешный вход в аккаунт: {aff_lucky_login}')

        # ! переход во вкладку c нужными данными
        driver.get('STATISTICS_URL')
        time.sleep(random.randint(2, 5))
        WebDriverWait(driver, 30).until(ec.visibility_of_element_located((By.CSS_SELECTOR, '[data-test-id="statistics-filter-form"]')))

        # ! Устанавливаем актуальную дату
        start_field = driver.find_element(By.XPATH, '//*[@id="js-scroll-container"]/main/div/div/div[1]/div/form/div[1]/div[1]/div[1]/div[1]/input')
        time.sleep(random.randint(2, 5))
        end_field = driver.find_element(By.XPATH, '//*[@id="js-scroll-container"]/main/div/div/div[1]/div/form/div[1]/div[1]/div[1]/div[3]/input')
        time.sleep(random.randint(2, 5))
        start_date_value = start_field.get_attribute('value')
        time.sleep(random.randint(2, 5))
        end_date_value = end_field.get_attribute('value')
        time.sleep(random.randint(2, 5))
        parse_date = f'{start_date_value.replace("-", ".")}-{end_date_value.replace("-", ".")}'
        time.sleep(random.randint(2, 5))

        # ! Получаем все оферы
        try:
            get_current_selector('Смартлинки').click()
        except:
            get_current_selector('Smartlinks').click()
        WebDriverWait(driver, 30).until(ec.visibility_of_element_located((By.CLASS_NAME, 'rc-virtual-list')))
        all_offers = driver.find_elements(By.CSS_SELECTOR, '.rc-virtual-list .ant-select-item-option-content')
        time.sleep(random.randint(2, 5))

        # ! Проходимся по каждому оферу
        team_data = []
        for i, offer in enumerate(all_offers):
            offer.click()
            time.sleep(random.randint(2, 5))
            team_data.extend(parse_data())
            offer.click()
        logging.info(f'Успешное получение данных для команды: {aff_lucky_login}')

        all_data[team_name] = team_data
        all_data["parse_date"] = parse_date
            
    except Exception as e:
        logging.error(f'Ошибка аккаунта {aff_lucky_login}: {str(e)}')
        #! Создание папки для сохранения ошибок
        current_date = datetime.now().strftime('%Y-%m-%d')
        output_dir = os.path.join('./previous-tables', current_date)
        os.makedirs(output_dir, exist_ok=True)
        
        #! Сохранение скриншота ошибки
        screenshot_path = os.path.join(output_dir, f'ERROR_{team_name}.png')
        driver.save_screenshot(screenshot_path)
        
        #! Запись ошибки в текстовый файл
        error_file_path = os.path.join(output_dir, 'ERROR.txt')
        with open(error_file_path, 'a', encoding='utf8') as error_file:
            error_file.write(f'Ошибка на сайте команды: {team_name}\n')
            error_file.write(f'Ошибка по адресу: {driver.current_url}\n')
            if 'GetHandleVerifier' in str(e):
                error_file.write(f'Ошибка: Нужно ввести капчу\n')
            error_file.write('\n')
        
        # Добавление пустых данных для команды с ошибкой
        all_data[team_name] = []
        all_data["parse_date"] = parse_date

    time.sleep(10)
            
    driver.get(aff_lucky_url)

driver.quit()

#! Сохраняем все данные в один JSON файл
with open('./table-data.json', 'w', encoding='utf8') as json_file:
    json.dump(all_data, json_file, ensure_ascii=False, indent=4)
    
#! Генерация таблицы и сохранение в папку с актуальной датой
current_date = datetime.now().strftime('%Y-%m-%d')
output_dir = os.path.join('./previous-tables', current_date)
os.makedirs(output_dir, exist_ok=True)
json_file_path = os.path.join(output_dir, './table-data.json')
with open(json_file_path, 'w', encoding='utf8') as json_file:
    json.dump(all_data, json_file, ensure_ascii=False, indent=4)
    
#! Генерация таблицы и сохранение в файл с актуальной датой
file_path = os.path.join(output_dir, f'{current_date}.xlsx')
generate_excel_table(all_data, file_path)


'''
Удаление старых папок, если их больше 12
В счет идут только те которые соответствуют дате в формате гггг-мм-дд
'''
date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
all_dirs = sorted(
    [d for d in os.listdir('./previous-tables') if os.path.isdir(os.path.join('./previous-tables', d)) and date_pattern.match(d)],
    key=lambda x: datetime.strptime(x, '%Y-%m-%d')
)
if len(all_dirs) > 12:
    shutil.rmtree(os.path.join('./previous-tables', all_dirs[0]))
    
# ! Начало записи в гугл
with open('./table-data.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# Получение заголовков из данных
headers = []
for section_data in data.values():
    if isinstance(section_data, list) and section_data:
        headers = list(section_data[0].keys())
        break

parse_date = data.get("parse_date", "")

# Создание нового листа с актуальной датой
current_date = datetime.now().strftime('%Y-%m-%d')
worksheet = None

#! Проверка существования листа с текущей датой
try:
    worksheet = spreadsheet.worksheet(current_date)
except gspread.exceptions.WorksheetNotFound:
    worksheet = spreadsheet.add_worksheet(title=current_date, rows="1500", cols="20")


# Добавление заголовка с parse_date
worksheet.append_row([parse_date] + [""] * 9)
date_range = 'A1:I1'
worksheet.merge_cells(date_range)
worksheet.format(date_range, {
    'horizontalAlignment': 'CENTER',
    'verticalAlignment': 'MIDDLE',
    'textFormat': {'bold': True, 'fontSize': 16},
    'backgroundColor': {'red': 179/255, 'green': 179/255, 'blue': 1/255}
})

#! Добавление данных в таблицу
add_data_to_sheet(data, worksheet)

#! Установка ширины колонок по содержимому
for col_num, header in enumerate(["Sprint"] + headers, start=1):
    max_len = max(len(header), 10)    

logging.info('Скрипт завершен')
asyncio.run(send_telegram_message(f"Скрипт завершен {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"))
