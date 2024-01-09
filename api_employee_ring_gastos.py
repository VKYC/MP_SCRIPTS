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
    for expense in _expenses:
        try:
            employee_id = int(expense['EmployeeNumber']) if expense['EmployeeNumber'] else "-1"
            db1_cursor.execute(
                f"INSERT INTO mp_usuarios (empleado_id, usuario_ext_id, email, identificacion, posicion, fecha_creacion) VALUES "
                f"({employee_id}, {expense['Id']}, '{expense['Email']}', '{expense['Identification']}', '{expense['Position']}', '{expense['CreatedAt']}');")

        except psycopg2.IntegrityError as e:
            # Si se produce un error de integridad, asumimos que el ID ya existe, entonces actualizamos en lugar de insertar
            employee_id = int(expense['EmployeeNumber']) if expense['EmployeeNumber'] else "-1"
            db1_conn.rollback()
            db1_cursor.execute(f"UPDATE mp_usuarios SET empleado_id = {employee_id},"
                               f" email = '{expense['Email']}', identificacion = '{expense['Identification']}', "
                               f"posicion = '{expense['Position']}', fecha_creacion = '{expense['CreatedAt']}'"
                               f" WHERE usuario_ext_id = '{expense['Id']}' ;")

        db1_conn.commit()
    db1_conn.close()
# URL base de la API
url_base = "https://api.rindegastos.com/v1"

# Ruta específica dentro de la API
ruta_especifica = "/getUsers"

# URL completa para la solicitud
url_completa = url_base + ruta_especifica
url_pagination = url_base + "/getUsers"

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
    process_expenses_date(datos_json['Users'])
    for i in range(2, (datos_json['Records']['Pages'] + 1)):
        request = requests.get(url_pagination + f'?Page={i}', headers=headers)
        json_result = request.json()
        process_expenses_date(json_result['Users'])
else:
    print(f"La solicitud falló con el código de estado: {respuesta.status_code}")
