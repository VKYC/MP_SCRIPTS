import psycopg2
from datetime import date

# Conexión a la base de datos PostgreSQL (mp_seadte)
db1_conn = psycopg2.connect(
    dbname='mp_seadte',
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
query = 'select rzn_soc_emsor, rut_emisor, folio, monto_total, fecha_emision, fecha_vencimiento, orden_compra, ' \
        'fecha_sii from "recepcion_dtes";'
db1_cursor.execute(query)
result_db1 = db1_cursor.fetchall()
for rec in result_db1:
    query_validator = f"select * from mp_facturas_conciliacion where rut_emisor = '{rec[1]}' and folio = {rec[2]}"
    db2_cursor.execute(query_validator)
    result_db2 = db2_cursor.fetchall()
    if not result_db2:
        query_insert = f"INSERT INTO mp_facturas_conciliacion (" \
                       f"rzn_soc_emsor, rut_emisor, folio, monto_total, " \
                       f"fecha_emision, fecha_vencimiento, orden_compra, fecha_sii" \
                       f") VALUES ('{rec[0]}', '{rec[1]}', {rec[2]}, {rec[3]}, '{rec[4]}', '{rec[5]}', '{rec[6]}', " \
                       f"'{rec[7]}')"
        db2_cursor.execute(query_insert)
        db2_conn.commit()

# Cierra las conexiones
db1_conn.close()
db2_conn.close()
