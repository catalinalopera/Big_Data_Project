#Importar librerias necesarias
import pyodbc
import pandas as pd

def connect_to_db(driver, server, database, username, password):
    """
    String para conectar a la base de datos SQL y Sypnase Analytics.
    """
    conn_str = (
        f"Driver={{{driver}}};"
        f"Server={server};"
        f"Database={database};"
        f"Uid={username};"
        f"Pwd={password};"
        f"Encrypt=yes;"
        f"TrustServerCertificate=no;"
        f"Connection Timeout=30;"
    )
    return pyodbc.connect(conn_str)

def extract_data(conn, query):
    """
    Extraer los datos de la base de datos SQL - Gestion de Clientes Seguros XYZ
    """
    return pd.read_sql(query, conn)

def validate_phone(phone):
    """
    Validar que el número teléfonico sea valido
    """
    if phone.isdigit() and len(phone) == 10:
        return phone
    else:
        return "DatoIncorrecto"
    
def clean_transform_data(df):
    """
    Función para Transformar y Limpiar los datos de la fuente de origen para cargarlo al DataWareHouse
    """
    # Borrar Columnas cuyo nombre viene NULL
    df = df[df['Nombre'].notnull()]

    # Verificar los datos que llegan de otras columnas NULL y asignar un nuevo valor como se detalla acontinuación.
    df['Apellido'].fillna('SinDato', inplace=True)
    df['Direccion'].fillna('SinDato', inplace=True)
    df['Telefono'] = df['Telefono'].apply(validate_phone)
    df['EstadoCivil'].fillna('SinDato', inplace=True)
    df['Profesion'].fillna('SinDato', inplace=True)
    df['IngresoAnual'].fillna(0, inplace=True)
    df['IngresoAnual'] = pd.to_numeric(df['IngresoAnual'], errors='coerce')
    df['IngresoAnual'] = df['IngresoAnual'].apply(lambda x: x if x >= 0 else 0)  # Reemplazar los valores Negativos con Cero 
    df['Estado'].fillna('SinDato', inplace=True)

    # Normalizar los textos en la base de datos
    df['Nombre'] = df['Nombre'].str.strip().str.title()
    df['Apellido'] = df['Apellido'].str.strip().str.title()
    df['Direccion'] = df['Direccion'].str.strip().str.title()
    df['EstadoCivil'] = df['EstadoCivil'].str.strip().str.capitalize()
    df['Profesion'] = df['Profesion'].str.strip().str.title()
    df['Estado'] = df['Estado'].str.strip().str.capitalize()

    # Crear una nueva columna que contenga la combinación del Nombre + Apellido
    df['NombreCompleto'] = df['Nombre'] + ' ' + df['Apellido']

    # En el caso de que se presenten duplicados con la identificación del cliente, se proceden a borrar
    df.drop_duplicates(subset='ClienteID', keep='last', inplace=True)

    return df

def load_data(df, conn):
    """
    Función para cargar la data transformada en la tabla del datawarehouse
    """
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("""
        INSERT INTO mart.DimClientes (ClienteID, NombreCompleto, Direccion, Telefono, FechaNacimiento, Sexo, EstadoCivil, Profesion, IngresoAnual, FechaRegistro, Estado)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        row['ClienteID'], row['NombreCompleto'], row['Direccion'], row['Telefono'], row['FechaNacimiento'], row['Sexo'], row['EstadoCivil'], row['Profesion'], row['IngresoAnual'], row['FechaRegistro'], row['Estado'])
    conn.commit()

def main():
    # Datos de conexión a la base de datos del sistema de gestión de clientes
    client_db_driver = 'ODBC Driver 18 for SQL Server'
    client_db_server = 'tcp:segurosxyz.database.windows.net,1433'
    client_db_database = 'GestionClientesXYZ'
    client_db_username = 'sqladminuser2024'
    client_db_password = 'Segurosxyz.2024'

    # Datos de conexión al data warehouse en Azure Synapse Analytics
    dw_driver = 'ODBC Driver 18 for SQL Server'
    dw_server = 'tcp:workspacesegurosxyz.sql.azuresynapse.net,1433'
    dw_database = 'segurosxyz_sqlpool'
    dw_username = 'sqladminuser'
    dw_password = 'Segurosxyz.2024'

    # Conectar a la base de datos del sistema de gestión de clientes
    conn_clientes = connect_to_db(client_db_driver, client_db_server, client_db_database, client_db_username, client_db_password)
    query_clientes = "SELECT * FROM Clientes"
    df_clientes = extract_data(conn_clientes, query_clientes)

    # Limpiar y transformar los datos
    df_transformed = clean_transform_data(df_clientes)
    
    # Conectar al data warehouse en Azure Synapse Analytics
    conn_synapse = connect_to_db(dw_driver, dw_server, dw_database, dw_username, dw_password)
    load_data(df_transformed, conn_synapse)
    
    # Mensaje de confirmación
    print("Los datos se han copiado exitosamente.")

if __name__ == "__main__":
    main()