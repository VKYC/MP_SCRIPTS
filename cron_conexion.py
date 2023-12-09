import psycopg2
from datetime import date

# Conexión a la base de datos PostgreSQL (procesos)
db1_conn = psycopg2.connect(
    dbname='procesos_01',
    user='postgres',
    password='1234',
    host='localhost',
    port='5432'
)
db1_cursor = db1_conn.cursor()

# Conexión a la base de datos PostgreSQL (odoo)
db2_conn = psycopg2.connect(
    dbname='piloto_05v4',
    user='postgres',
    password='1234',
    host='localhost',
    port='5432'
)
db2_cursor = db2_conn.cursor()
hoy = date.today()

# Consulta en la base de datos PostgreSQL (procesos)
query = 'select valor from "INDICADORES_FINANCIEROS" WHERE fecha = ' + "'" + hoy.today().strftime("%Y-%m-%d") + "'" + ' and codigo_indicador = ' + "'" + 'dolar' + "';"
db1_cursor.execute(query)
result_db1 = db1_cursor.fetchall()

clp_per_unit = 1 / result_db1[0][0]

db2_cursor.execute('select id from res_currency where code = ' + "'" + 'US' + "'")
currency_id = db2_cursor.fetchall()[0][0]

db2_cursor.execute('INSERT INTO res_currency_rate (name, rate, currency_id, company_id) VALUES (' + "'" +
                   hoy.today().strftime("%Y-%m-%d") + "', " + str(clp_per_unit) + ', ' + str(currency_id) + ', 1);')
db2_conn.commit()

# Cierra las conexiones
db1_conn.close()
db2_conn.close()
