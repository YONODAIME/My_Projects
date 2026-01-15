import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from sqlalchemy import create_engine, text
from gspread_formatting import CellFormat, textFormat, format_cell_range
import settings

scope = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
client = gspread.service_account(filename='analytics-autotracker-276a3cecab72.json')

spreadsheet = client.open_by_key('1mc-yvLEG_yPJhHdA3Kuo2_LXbwAlbEhvrw2s2PZBVGk')
sheet = spreadsheet.worksheet('Группа Фомина')

user = settings.USER_SUMMARY
password = settings.PASSWORD_SUMMARY
host = settings.SERVER_SUMMARY
port = '3306'
database_raw = settings.DATABASE_RAW

query = """SELECT
CONCAT('https://portal.stavtrack.ru/crm/company/details/', company_id, '/') AS company_link,
company_name,
    last_name,
    CONCAT('https://portal.stavtrack.ru/crm/invoice/show/', order_id, '/') AS invoice_link,
    order_topic,
    COALESCE(SUM(receivable), 0) AS receivable,
    COALESCE(SUM(collected), 0) AS collected,
    currency,
    sale_group_name
FROM (
    SELECT
        b.yearmonth,
        b.order_id,
        c.id AS company_id,
        c.title AS company_name,
        bci.order_topic,

        -- Сумма подлежащая оплате
        CASE
            WHEN ((bci.payed = 'N' AND bci.pay_voucher_date IS NULL) OR
                  EXTRACT(YEAR_MONTH FROM bci.date_payed) = EXTRACT(YEAR_MONTH FROM CURDATE()))
            THEN CAST(b.price * b.quantity AS DECIMAL)
        END AS receivable,

        -- Сумма оплаченная в текущем месяце
        CASE
            WHEN ((bci.payed = 'Y' OR bci.pay_voucher_date IS NOT NULL) AND
                  EXTRACT(YEAR_MONTH FROM bci.date_payed) = EXTRACT(YEAR_MONTH FROM CURDATE()))
            THEN CAST(b.price * b.quantity AS DECIMAL)
        END AS collected,

        CASE
            WHEN buci.UF_CRM_1487336268 IS NULL THEN 'RUB'
            ELSE buci.UF_CRM_1487336268
        END AS currency,

        ug.sale_group_name,
        ug.last_name

    FROM (
        SELECT
            CASE
                WHEN DAY(CURDATE()) > 6 THEN EXTRACT(YEAR_MONTH FROM CURDATE())
                ELSE EXTRACT(YEAR_MONTH FROM CURDATE() - INTERVAL 1 MONTH)
            END AS yearmonth,
            order_id,
            product_id,
            name,
            price,
            quantity,
            discount_price
        FROM raw_data.b_crm_invoice_basket
        WHERE name LIKE '%абонентск%'
           OR name LIKE '%Доступ%сервер%'
           OR name LIKE '%подписк%сервис%'
           OR name LIKE '%wia%'
    ) AS b

    LEFT JOIN raw_data.b_crm_invoice AS bci ON bci.id = b.order_id
    LEFT JOIN raw_data.b_uts_crm_invoice AS buci ON buci.value_id = bci.id
    LEFT JOIN raw_data.b_crm_company AS c ON c.id = buci.uf_company_id

    LEFT JOIN (
        SELECT
            user_id,
            last_name,
            CASE
                WHEN sale_group_name IN (
                    'Группа Кудренко', 'Группа Ерастова', 'Группа Квицинии', 'Группа Фомина',
                    'Группа Ковалева', 'Группа Уртеновой', 'Группа Салженикиной', 'Хантеры', 'Офис Астана'
                ) THEN sale_group_name
                ELSE 'Другое'
            END AS sale_group_name
        FROM meta_data.bi_user_group
    ) AS ug ON ug.user_id = c.ASSIGNED_BY_ID

    RIGHT JOIN (
        SELECT
            year_month_key,
            REPLACE(year_month_name, ' ', '%') AS month_name
        FROM meta_data.dict_year_month
        WHERE year_month_key >= 202101
          AND year_month_key < EXTRACT(YEAR_MONTH FROM CURDATE())
    ) AS ym ON b.name LIKE CONCAT('%', ym.month_name, '%')

    WHERE bci.canceled = 'N'
      AND ((bci.payed = 'N' AND bci.pay_voucher_date IS NULL)
        OR EXTRACT(YEAR_MONTH FROM bci.date_payed) = EXTRACT(YEAR_MONTH FROM CURDATE()))
      AND (buci.UF_DEAL_ID IS NULL OR buci.UF_DEAL_ID = 0)
      AND buci.uf_company_id NOT IN (7488, 11930, 17751)
      AND buci.uf_company_id NOT IN (SELECT id FROM summary_data.big_client)
) AS t
WHERE sale_group_name = 'Группа Фомина'
GROUP BY
    yearmonth,
    sale_group_name,
    last_name,
    company_id,
    company_name,
    order_id,
    order_topic,
    currency
ORDER BY
    yearmonth,
    sale_group_name,
    last_name,
    company_id,
    currency DESC;
"""

engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{database_raw}")
df = pd.read_sql(text(query), engine)

numeric_cols = df.select_dtypes(include=['number']).columns
for col in df.columns:
    if col not in numeric_cols:
        df[col] = df[col].astype(str)

values = df.values.tolist()

sheet.resize(rows=len(values) + 1)
sheet.update('A2', values)

cell_format = CellFormat(textFormat=textFormat(bold=False, fontSize=10))
range_to_format = f"A2:Z{len(values)+1}"
format_cell_range(sheet, range_to_format, cell_format)



