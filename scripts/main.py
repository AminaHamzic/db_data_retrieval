from scripts.db_connect import load_config, create_db_engine, test_connection
from scripts.geo_data import fetch_geo_data, modify_geo_file
from scripts.data_retrieval import fetch_data,capitalize_level_names, rename_columns


if __name__ == "__main__":
    config = load_config()
    engine = create_db_engine(config)
    test_connection(engine)

    # 1. Fetch geo data
    fetch_geo_data(engine, config)
    modify_geo_file(config.get('geo_file', ''))

    # 2. Fetch data from tables
    # Capitalize sum level names
    geo_level_info = config['geoLevelInformation']

    geo_level_names = [k[1] for k in geo_level_info]
    geo_names_cap = capitalize_level_names(geo_level_names)

    print(geo_names_cap)

    fetch_data(engine, config, geo_names_cap, prefix= 'EASI2028M_EASI2028')

    rename_columns(r"C:\Projects\db_data_retrieval\datasets\EASI2028\dp_EASI2028\data",
                   r"C:\Projects\db_data_retrieval\datasets\EASI2028\dp_EASI2028\preprocessed_data")

