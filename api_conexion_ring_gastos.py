import requests
import psycopg2


def process_expenses_date(_expenses):
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
    for expense in _expenses:
        if expense['Status'] == 1:
            state = 'aprobado'
            try:
                query = f"select email from mp_usuarios WHERE usuario_ext_id = '{expense['UserId']}'"
                db1_cursor.execute(query)
                result_db1 = db1_cursor.fetchall()
                db1_conn.commit()
                # print(result_db1)
                query = f"select id from hr_employee WHERE work_email = '{result_db1[0][0]}'"
                db2_cursor.execute(query)
                result_db2 = db2_cursor.fetchall()
                db2_conn.commit()
                # print(result_db1[0][0])
                if result_db2:
                    # print(result_db2[0][0])

                    db2_cursor.execute(f'INSERT INTO mp_gastos (expense_ext_id, concepto, monto, empleado_ext_id, estado, estado_recuperado, empleado_id) VALUES'
                                       f' ({expense["Id"]}, ' + "'" + expense["Category"] + "'" + f', {expense["Total"]}, {expense["UserId"]}, ' + "'" + state + "'" + ',' + "'" + 'no_recuperado' + "'"  + f', {result_db2[0][0]});')

            except psycopg2.IntegrityError as e:
                # Si se produce un error de integridad, asumimos que el ID ya existe, entonces actualizamos en lugar de insertar
                db2_cursor.rollback()
                print(e)
                # db2_cursor.execute(f'UPDATE mp_gastos SET concepto = ' + "'" + expense["Category"] + "'" + f', monto = {expense["Total"]}, empleado_id = {expense["UserId"]}, estado = ' + "'" + state + "'" + f' WHERE expense_ext_id = {expense["Id"]};')

        db2_conn.commit()
        db1_conn.commit()
    db2_conn.close()
    db1_conn.close()
# URL base de la API
url_base = "https://api.rindegastos.com/v1"

# Ruta específica dentro de la API
ruta_especifica = "/getExpenses?Currency=CLP"

# URL completa para la solicitud
url_completa = url_base + ruta_especifica

# Clave de autorización
clave_autorizacion = ""

# Encabezados de la solicitud con la clave de autorización
headers = {
    "Authorization": f"Bearer {clave_autorizacion}"
}

# Realizar la solicitud GET sin verificar SSL
respuesta = requests.get(url_completa, headers=headers)

# Verificar el código de estado de la respuesta
if respuesta.status_code == 200:
    # La solicitud fue exitosa, puedes trabajar con la respuesta en formato JSON
    datos_json = respuesta.json()
    process_expenses_date(datos_json['Expenses'])
    for i in range(2, (datos_json['Records']['Pages'] + 1)):
        request = requests.get(url_completa + f'&Page={i}', headers=headers)
        json_result = request.json()
        process_expenses_date(json_result['Expenses'])
else:
    print(f"La solicitud falló con el código de estado: {respuesta.status_code}")
