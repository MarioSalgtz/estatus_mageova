import requests
import os
import psycopg2
import pyodbc
from datetime import datetime, timedelta
from dotenv import load_dotenv


# Cargar las variables de entorno desde .env
load_dotenv()

# Token API de ZeroTier
api_token = os.getenv('ZEROTIER_API_TOKEN', 'zHq08bFlhsDe4bSdd2UP7FFkEA7hYWEe')

# Lista de Network IDs
network_ids = [
    '233ccaac2779a42d',
    '93afae59632c01f8',
    '856127940cade10e'
]

# Cabeceras de la solicitud (autenticación)
headers = {
    'Authorization': f'bearer {api_token}'
}

# Configuración de la base de datos PostgreSQL
db_config = {
    "host": "mageova-postgres.ccfjejrsiibd.us-east-2.rds.amazonaws.com",
    "port": "5432",
    "database": "Mageova_New",
    "user": "mageova",
    "password": "Jalisconoterajes"
}

# Configuración de los servidores SQL Server (agregar los dos adicionales)
sql_server_configs = [
    {
        "server": "98.142.102.114,1433",  # Servidor original
        "database": "WifiPlatform_Standard",
        "username": "consultas",
        "password": "2024**Mageova"
    },
    {
        "server": "138.128.160.66,1433",  # Nuevo servidor
        "database": "WifiPlatform_Standard",
        "username": "consultas",
        "password": "2024**Mageova"
    },
    {
        "server": "198.136.56.82,1433",  # Nuevo servidor
        "database": "WifiPlatform_Standard",
        "username": "consultas",
        "password": "2024**Mageova"
    }
]

# Conexión a PostgreSQL
conn_pg = psycopg2.connect(**db_config)
cursor_pg = conn_pg.cursor()

# Función para conectar a SQL Server
def conectar_sql_server(config):
    conn_sql = pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={config['server']};"
        f"DATABASE={config['database']};"
        f"UID={config['username']};"
        f"PWD={config['password']}"
    )
    return conn_sql

# Función para verificar y actualizar el estatus en PostgreSQL
def actualizar_estatus_en_pg(device, estatus, estatus_fecha, columna):
    if estatus == 'YES':
        query = f"""
            UPDATE disp_1
            SET estatus = %s, estatus_fecha = %s
            WHERE {columna} = %s
        """
        cursor_pg.execute(query, (estatus, estatus_fecha, device))
    else:
        query = f"""
            UPDATE disp_1
            SET estatus = %s
            WHERE {columna} = %s
        """
        cursor_pg.execute(query, (estatus, device))
    
    conn_pg.commit()

# Función para obtener el estado de los routers desde SQL Server
def obtener_estado_routers_sql(cursor_sql):
    query = "SELECT D_code, D_state FROM DeviceInfo ORDER BY 1"
    cursor_sql.execute(query)
    return cursor_sql.fetchall()

# Función para obtener los miembros de una red y sus IPs desde ZeroTier y actualizar PostgreSQL
def obtener_miembros_de_red(network_id):
    url = f'https://my.zerotier.com/api/network/{network_id}/member'
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        print(f"Miembros de la red {network_id}:")
        
        for member in data:
            member_id = member['id']
            ip_assignments = member['config'].get('ipAssignments', [])  # IP asignada
            last_seen = member.get('lastOnline', None)  # Última vez visto
            
            # Procesar miembros sin IP asignada (inactivos)
            if not ip_assignments:
                print(f'Miembro sin IP asignada: {member_id}')
            
            # Procesar miembros sin `lastOnline` (inactivos)
            if last_seen:
                last_seen_time = datetime.fromtimestamp(last_seen / 1000)
                tiempo_actual = datetime.now()
                tiempo_desde_last_seen = tiempo_actual - last_seen_time
            else:
                print(f'Miembro no visto recientemente: {member_id}')
                last_seen_time = None
                tiempo_desde_last_seen = None

            # Si el dispositivo está en la base de datos, actualizar el estatus
            for ip in ip_assignments:
                cursor_pg.execute("SELECT router_ip FROM disp_1 WHERE router_ip = %s", (ip,))
                resultado_pg = cursor_pg.fetchone()

                if resultado_pg:
                    # Revisar si el dispositivo ha estado activo en la última hora
                    if tiempo_desde_last_seen and tiempo_desde_last_seen <= timedelta(hours=1):
                        estatus = 'YES'  # Online (menos de una hora)
                    else:
                        estatus = 'NO'  # Offline (más de una hora o inactivo)
                    
                    estatus_fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    actualizar_estatus_en_pg(ip, estatus, estatus_fecha, 'router_ip')
                    print(f'  IP: {ip}, Estatus actualizado a: {estatus}, Fecha: {estatus_fecha}')
    else:
        print(f'Error al obtener datos de la red {network_id}: {response.status_code}')

# Función para comparar con los datos de SQL Server y actualizar PostgreSQL
def comparar_con_sql_server():
    for config in sql_server_configs:
        conn_sql = conectar_sql_server(config)
        cursor_sql = conn_sql.cursor()

        try:
            estados_sql = obtener_estado_routers_sql(cursor_sql)
            
            for d_code, d_state in estados_sql:
                cursor_pg.execute("SELECT device FROM disp_1 WHERE device = %s", (d_code,))
                resultado_pg = cursor_pg.fetchone()
                
                if resultado_pg:
                    estatus = 'YES' if d_state == 1 else 'NO'
                    estatus_fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    actualizar_estatus_en_pg(d_code, estatus, estatus_fecha, 'device')
                    print(f'Device: {d_code}, Estatus actualizado a: {estatus} en PostgreSQL.')
        
        finally:
            cursor_sql.close()
            conn_sql.close()

# Ejecutar ZeroTier y luego comparar con SQL Server
for network_id in network_ids:
    obtener_miembros_de_red(network_id)

# Comparar con SQL Server en los tres servidores
comparar_con_sql_server()

# Cerrar las conexiones a PostgreSQL
cursor_pg.close()
conn_pg.close()
