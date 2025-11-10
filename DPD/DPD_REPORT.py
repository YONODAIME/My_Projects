import requests
import pandas as pd
import io
from datetime import datetime, timedelta
from sqlalchemy import create_engine

import settings

user = settings.USER_SUMMARY
passwd = settings.PASSWORD_SUMMARY
host = settings.SERVER_SUMMARY
port = '3306'
database_raw = settings.DATABASE_RAW

today = datetime.today()
three_months_ago = today - timedelta(days=90)

print("Начальная дата:", three_months_ago.date())
print("Конечная дата:", today.date())

session = requests.Session()
# Session хранит куки и заголовки между запросами

username = settings.DPD_LOGIN
password = settings.DPD_PASSWORD
clientNumber = settings.DPD_CLIENT_NUMBER

credentials = {
    "username": username,
    "password": password,
    "countryCode": "RU",
    "clientNumber": clientNumber
}

auth_url = "https://dpd-mydpd-auth-backend-prod.dpd.ru/api/v1/login"
auth_response = session.post(auth_url, json=credentials)

if auth_response.status_code == 200:
    token = auth_response.json().get("token") or auth_response.json().get("access_token")
    if not token:
        print("Токен не найден, вот ответ сервера:")
        print(auth_response.text)
        exit()
    print("Авторизация успешна")
else:
    print("Ошибка авторизации:", auth_response.status_code, auth_response.text)
    exit()

export_url = "https://dpd-mydpd-order-backend-api-prod.dpd.ru/api/v1/order/list/export/excel"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

payload = {
    "productIds": [],
    "clientOrderNumFlag": False,
    "vdoOrder": False,
    "ssd": False,
    "orderStates": ["REQ", "CREATED", "PICKED_UP", "CANCELED_BY_REGISTRY", "CANCELED",
                    "ON_THE_WAY", "READY_FOR_PICKUP", "READY_FOR_PICKUP_EXPIRED",
                    "PAYMENT_REQUIRED", "DELIVERED", "ANOTHER_ORDER", "PARTIALLY_DELIVERED",
                    "CLARIFICATION", "RETURN", "SERVICE_NOT_PROVIDED", "CARGO_LOST",
                    "CUSTOMS_CLEARANCE", "PARTIALLY_PURCHASED", "RETURNED_TO_SENDER",
                    "DELIVERY_PROBLEMS", "CLIENT_CANCEL"],
    "showAll": True,
    "startDate": three_months_ago.strftime("%Y-%m-%d"),
    "endDate": today.strftime("%Y-%m-%d")
}

all_data = []
page = 1
rows_on_page = 1000

while True:
    payload["pageNum"] = page
    payload["rowsOnPage"] = rows_on_page
    resp = session.post(export_url, headers=headers, json=payload)

    if resp.status_code != 200:
        print(f"Ошибка на странице {page}:", resp.status_code, resp.text)
        break

    df_page = pd.read_excel(io.BytesIO(resp.content))

    if df_page.empty:
        print(f"Данных на странице {page} больше нет, заканчиваем")
        break

    all_data.append(df_page)
    print(f"Страница {page} загружена, строк: {len(df_page)}")

    if len(df_page) < rows_on_page:
        print("Последняя страница получена")
        break

    page += 1

df = pd.concat(all_data, ignore_index=True)

df.to_excel("dpd_orders_preview.xlsx", index=False)

df.columns = [
    'date_receive', 'client_order_number', 'dpd_order_number', 'sender',
    'receiver', 'service', 'status', 'price_rub', 'date_delivery', 'payer_not_client'
]
df_to_db = df.drop(columns=['client_order_number', 'payer_not_client'])
engine = create_engine(f"mysql+pymysql://{user}:{passwd}@{host}:{port}/{database_raw}")
df_to_db.to_sql("DPD_orders", con=engine, if_exists="append", index=False)
print("Данные успешно загружены в MySQL")
