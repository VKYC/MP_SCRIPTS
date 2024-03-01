import pandas as pd
import psycopg2
import ast

# Conexión a la base de datos PostgreSQL (odoo)
db2_conn = psycopg2.connect(
    dbname='erp_07',
    user='postgres',
    password='1234',
    host='localhost',
    port='5432'
)
db2_cursor = db2_conn.cursor()
ruta_archivo_excel = 'res_currency_rate.xlsx'

df = pd.read_excel(ruta_archivo_excel)

for index, row in df.iterrows():
    date = row['name']
    unity_by_origin_currency = row['rate']
    origin_currency_by_unity = row['inverse_company_rate']
    currency_array_name = ast.literal_eval(row['currency_id'])[1]
    date_list = []
    if date >= '2023-08-01' and currency_array_name != 'CLP':

        db2_cursor.execute('select id from res_currency where name = ' + "'" + currency_array_name + "'")
        currency_id = db2_cursor.fetchall()[0][0]
        # print(f'{date}, {unity_by_origin_currency}, {origin_currency_by_unity}, {currency_array_name}, {currency_id}')

        try:
            db2_cursor.execute('INSERT INTO res_currency_rate (name, rate, currency_id, company_id) VALUES (' + "'" +
                               date + "', " + str(unity_by_origin_currency) + ', ' + str(currency_id) + ', 1);')

        except:
            # db2_cursor.rollback()
            pass
        db2_conn.commit()
db2_conn.close()
