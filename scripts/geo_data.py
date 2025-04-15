import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import re


def fetch_geo_data(engine, config):
    """
    Fetch tables for output as geo_identifier.csv.
    """
    try:
        geo_file_path = config.get('geo_file_path', '')
        if not geo_file_path:
            print("Geo_file_path not found in config.")
            return

        # Ensure the directory exists
        if not os.path.exists(geo_file_path):
            print(f"Directory {geo_file_path} does not exist. Creating it.")
            os.makedirs(geo_file_path)

        output_path = os.path.join(geo_file_path, 'geo_identifiers.csv')

        # Check if the geo_identifier file already exists
        if os.path.exists(output_path):
            print(f"File {output_path} already exists. Skipping file creation.")
        else:
            print(f"Creating file: {output_path}")

        with engine.connect() as conn:
            tables_query = text("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_NAME LIKE '%_001'
            """)
            result = conn.execute(tables_query).fetchall()

            if not result:
                print("No tables with '_001' found.")
                return

            # Fetch data from each table
            for row in result:
                table_schema, table_name = row[0], row[1]
                full_table_name = f"{table_schema}.{table_name}"
                print(f"Fetching data from: {full_table_name}")

                try:
                    match = re.search(r'SL\d{3}', table_name)
                    if match:
                        geo_level = match.group(0)
                    else:
                        print(f"Geo level not found in table name: {table_name}")
                        continue

                    data_query = text(f"SELECT FIPS, Name FROM {full_table_name}")
                    result = conn.execute(data_query)
                    columns = result.keys()
                    data = result.fetchall()

                    df = pd.DataFrame(data, columns=columns).astype(str)
                    df['_geo_level_'] = geo_level
                    df['source_table'] = table_name

                    if not os.path.exists(output_path):
                        df.to_csv(output_path, index=False)
                    else:
                        df.to_csv(output_path, mode='a', header=False, index=False)

                except Exception as fetch_error:
                    print(f"Error fetching data from {full_table_name}: {fetch_error}")

            print(f"geo_identifier.csv updated at {output_path}")

    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error fetching geo data: {e}")

    except Exception as ex:
        print(f"Unexpected error: {ex}")


def modify_geo_file(input_file_path):
    """
    Modify the geo_identifier.csv file to drop column 'source_table'.
    Rename FIPS -> _geoid_
    """
    df = pd.read_csv(input_file_path, dtype=str)
    df.drop(columns=['source_table'], inplace=True)
    df.drop_duplicates(inplace=True)

    df.rename(columns={'FIPS': '_geoid_', 'Name': 'NAME'}, inplace=True)
    df.to_csv(input_file_path, index=False)
    print(f"Modified {input_file_path}")




