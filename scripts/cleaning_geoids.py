import os
import csv


def filter_data_by_geo(geo_file, data_file, output_folder, target_geo_level):
    """
    Filters the data file by comparing its geoids with the allowed geoids in the geo file.
    Only records with the target geo_level are checked, and if their geoid is not present in the
    geo file, they are removed. All other records are kept.

    Parameters:
        geo_file (str): Path to the geo input file containing allowed geoids (one per line).
        data_file (str): Path to the data input file (CSV format) with at least columns 'geo_level' and 'geoid'.
        output_folder (str): Path to the folder where the filtered data file will be written.
        target_geo_level (str): The geo_level (e.g., "SL900") to use for filtering.
    """
    # Read allowed geoids from geo_file into a set for fast look-up.
    with open(geo_file, 'r', encoding='utf-8') as gf:
        allowed_geoids = {line.strip() for line in gf if line.strip()}

    # Ensure the output folder exists.
    os.makedirs(output_folder, exist_ok=True)
    output_file = os.path.join(output_folder, os.path.basename(data_file))

    # Open the data file and output file.
    with open(data_file, 'r', encoding='utf-8') as df, \
            open(output_file, 'w', encoding='utf-8', newline='') as of:

        reader = csv.DictReader(df)
        writer = csv.DictWriter(of, fieldnames=reader.fieldnames)

        # Write header to output file.
        writer.writeheader()

        for row in reader:
            # Only filter records with the specified geo_level.
            if row.get("_geo_level_") == target_geo_level:
                # If the geoid is not in allowed_geoids, skip this record.
                if row.get("_geoid_") not in allowed_geoids:
                    continue
            # Write the record (filtered or unfiltered) to the output.
            writer.writerow(row)

# Example usage:
filter_data_by_geo(
    r"C:\Projects\db_data_retrieval\datasets\POLIS_CO_2022\dp_POLIS_CO_2022\preprocessed_data\geo_identifiers.csv",
    r"C:\Projects\db_data_retrieval\datasets\POLIS_CO_2022\ORGELE_USGE_001.csv",
    r"C:\Projects\db_data_retrieval\datasets\POLIS_CO_2022\TEST",
    "SL040")