import urllib
from sqlalchemy import create_engine

def create_dw_engine(settings):
    db_config = settings.get("data_warehouse", {})

    server = db_config.get("server")
    database = db_config.get("database")
    driver = db_config.get("driver", "ODBC Driver 17 for SQL Server")
    trusted_connection = db_config.get("trusted_connection", True)

    connection_string = f"""
    DRIVER={{{driver}}};
    SERVER={server};
    DATABASE={database};
    Trusted_Connection={"yes" if trusted_connection else "no"};
    """

    params = urllib.parse.quote_plus(connection_string)
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def create_sql_server_engine(settings):
    db_config = settings.get("database", {})

    server = db_config.get("server")
    database = db_config.get("database")
    driver = db_config.get("driver", "ODBC Driver 17 for SQL Server")
    trusted_connection = db_config.get("trusted_connection", True)

    if not server or not database:
        raise ValueError("Faltan parámetros de conexión de base de datos en config.json")

    connection_string = f"""
    DRIVER={{{driver}}};
    SERVER={server};
    DATABASE={database};
    Trusted_Connection={"yes" if trusted_connection else "no"};
    """

    params = urllib.parse.quote_plus(connection_string)
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={params}",
        fast_executemany=True
    )
    return engine