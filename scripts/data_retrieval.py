import os
import pandas as pd
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
import re


def fetch_data(engine, config, geo_names_cap, prefix):
    """
    Fetch data from tables and append to CSV files with specified prefix.
    """
    try:
        data_folder = config.get('data_folder', '')
        if not data_folder:
            print("Data folder not found in config.")
            return

        # Ensure the directory exists
        if not os.path.exists(data_folder):
            print(f"Directory {data_folder} does not exist. Creating it.")
            os.makedirs(data_folder)

        # Extract geo levels from the configuration
        #geo_level = [level[0] for level in config['geoLevelInformation']]

        with engine.connect() as conn:
            tables_query = text("""
                SELECT TABLE_SCHEMA, TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                  AND TABLE_NAME NOT LIKE '%Names%'
                  AND (
                        TABLE_NAME LIKE '%_001' OR
                        TABLE_NAME LIKE '%_002' OR
                        TABLE_NAME LIKE '%_003' OR
                        TABLE_NAME LIKE '%_043'
                      );
            """)
            result = conn.execute(tables_query).fetchall()

            if not result:
                print("No tables found.")
                return

            # Separate tables into two groups based on the presence of 'U' after the second underscore

            #tables_with_u = [row for row in result if re.search(r'^[^_]+_[^_]+_U_', row[1])]
            #tables_without_u = [row for row in result if not re.search(r'^[^_]+_[^_]+_U_', row[1])]

            # Process tables without 'U'
            #process_tables(tables_without_u, engine, data_folder, geo_names_cap, prefix)

            # Process tables with 'U'
            process_tables(result, engine, data_folder, geo_names_cap, prefix)

    except SQLAlchemyError as e:
        print(f"SQLAlchemy Error fetching data: {e}")

    except Exception as ex:
        print(f"Unexpected error: {ex}")


def process_tables(tables, engine, data_folder, geo_names_cap, prefix):
    """
    Process and fetch data from the given list of tables.
    """
    for row in tables:
        table_schema, table_name = row[0], row[1]
        full_table_name = f"{table_schema}.{table_name}"
        print(f"Fetching data from: {full_table_name}")

        try:
            table_suffix = table_name[-3:]
            if not table_suffix.isdigit():
                print(f"Table name does not end with three digits: {table_name}")
                continue

            data_query = text(f"SELECT * FROM {full_table_name}")
            result = engine.execute(data_query)
            columns = result.keys()
            data = result.fetchall()

            df = pd.DataFrame(data, columns=columns).astype(str)

            # Exclude specific columns, but keep FIPS
            exclude_columns = ['Name', 'QName'] + [col for col in columns if re.match(r'SL\d{3}_(FIPS|NAME)', col)] + list(geo_names_cap)
            df.drop(columns=exclude_columns, inplace=True, errors='ignore')

            # Generate output file name
            output_file_name = f"{prefix}_{table_suffix}.csv"
            output_path = os.path.join(data_folder, output_file_name)

            # Append data to the CSV file
            if os.path.exists(output_path):
                df.to_csv(output_path, mode='a', header=False, index=False)
            else:
                df.to_csv(output_path, index=False)
            print(f"Data appended to {output_path}")

        except Exception as fetch_error:
            print(f"Error fetching data from {full_table_name}: {fetch_error}")



def capitalize_level_names(geo_level):
    """
    This function returns list of geo names capitalized (NATION, COUNTY...)
    :param geo_level:
    :return: List of geo level names capitalized
    """
    geo_names_cap = []
    chars_for_removal = [
        '(', ')', '&', '/', ',', '.',
        '\\', '\'', '&', '%', '#',
    ]

    for gl in geo_level:
        for ch in chars_for_removal:
            if ch in gl:
                gl = gl.replace(ch, ' ')
        gl = gl.strip()
        while '  ' in gl:
            gl = gl.replace('  ', '')
        if ' ' in gl:
            blank_pos = [i for i, letter in enumerate(gl) if letter == ' ']
            geo_names_cap.append(
                gl[0].upper() + ''.join([
                    gl[i + 1]
                    for i in blank_pos
                ]).upper(),
            )
        else:
            geo_names_cap.append(gl.upper())

    return geo_names_cap


def rename_columns(input_folder: str, output_folder: str):
    """
    Rename columns GeoID -> _geoid_, SUMLEV -> _geo_level_ in data files.
    Rename them based on:
    T001_reg -> POLCEN2024_001, T002_reg -> POLCEN2024_002, etc.
    """
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    for file in os.listdir(input_folder):
        if file.endswith('.csv'):
            input_path = os.path.join(input_folder, file)
            df = pd.read_csv(input_path, dtype=str, on_bad_lines='skip')
            df.rename(columns={'FIPS': '_geoid_', 'SUMLEV': '_geo_level_'}, inplace=True)

            # Replace NaN values with empty strings
            df.fillna('', inplace=True)
            df.replace('None''nan', '', inplace=True)


            new_filename = f"{file}"
            output_path = os.path.join(output_folder, new_filename)

            df.to_csv(output_path, index=False)
            #print(f"Processed {file} -> {new_filename}")

