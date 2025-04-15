import yaml
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError


def load_config(config_path='../config/EASI2028/config.yml'): # TODO: Change the path to the config file
    """Load config"""
    with open(config_path, 'r') as file:
        return yaml.safe_load(file)


def list_odbc_drivers():
    """List available ODBC drivers."""
    return pyodbc.drivers()


def create_db_engine(config):
    """Create SQLAlchemy engine using dynamic driver selection from config."""
    server = config.get('serverName')
    database = config.get('databaseName')
    username = config.get('dbUsername')
    password = config.get('dbPassword')
    dialect = config.get('sqlAlchemyDialect', 'mssql+pyodbc')
    driver = config.get('sqlServerDriverName', 'ODBC Driver 17 for SQL Server')

    # Check if driver exists
    available_drivers = list_odbc_drivers()
    if driver.replace('+', ' ') not in available_drivers:
        print("Specified driver not found. Available drivers are:")
        for drv in available_drivers:
            print(f" - {drv}")
        raise Exception(f"ODBC Driver '{driver}' not found. Please update config.yml")

    # Build connection string
    connection_str = f"{dialect}://{username}:{password}@{server}/{database}?driver={driver}"
    return create_engine(connection_str)


def test_connection(engine):
    """Test the database connection"""
    try:
        with engine.connect() as conn:
            print("Connection successful!")

            # Current DB info
            db_query = text("SELECT DB_NAME() AS CurrentDatabase")
            db_result = conn.execute(db_query).fetchone()
            print(f"Connected to Database: {db_result['CurrentDatabase']}")

            # Number of tables
            tables_query = text("""
                SELECT COUNT(*) AS TableCount
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
            """)
            table_count = conn.execute(tables_query).fetchone()['TableCount']
            print(f"Total Tables: {table_count}")

            # First 5 table names
            list_tables_query = text("""
                SELECT TOP 5 TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            tables = conn.execute(list_tables_query).fetchall()
            print("Sample Tables:")
            for table in tables:
                print(f" - {table['TABLE_SCHEMA']}.{table['TABLE_NAME']}")

            # Current Server Time
            time_query = text("SELECT CURRENT_TIMESTAMP AS ServerTime")
            server_time = conn.execute(time_query).fetchone()['ServerTime']
            print(f"Server Time: {server_time}")

    except SQLAlchemyError as e:
        print(f"Connection failed: {e}")



