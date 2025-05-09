import arcpy
import requests
import csv

def extract():
    print("Calling extract function...")
    url = "https://docs.google.com/spreadsheets/d/e/2PACX-1vTDjitOlmILea7koCORJqk6QrUcwBJM7K3vy4guXBOmU_nWR6wsPn136bpH6ykoUxyTMW7wTwkzE371/pub?output=csv"

    try:
        r = requests.get(url)
        r.raise_for_status()  # Raise an error for bad status codes
        r.encoding = "utf-8"

        with open(r"C:\Users\Spencer\Downloads\addresses.csv", "w", encoding="utf-8") as output_file:
            output_file.write(r.text)

        print("Extract complete. Data saved to addresses.csv")
    except requests.RequestException as e:
        print(f"Error fetching data: {e}")


def transform():
    print("Adding City, State and Geocoding addresses...")

    input_file = r"C:\Users\Spencer\Downloads\addresses.csv"
    output_file = r"C:\Users\Spencer\Downloads\new_addresses.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as transformed_file:
        transformed_file.write("X,Y,Type\n")

        with open(input_file, "r", encoding="utf-8") as partial_file:
            csv_dict = csv.DictReader(partial_file, delimiter=',')

            for row in csv_dict:
                address = f"{row['Street Address']} Boulder CO"
                print(f"Geocoding: {address}")

                geocode_url = f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress?address={address}&benchmark=2020&format=json"

                try:
                    r = requests.get(geocode_url)
                    r.raise_for_status()
                    resp_dict = r.json()

                    matches = resp_dict.get('result', {}).get('addressMatches', [])
                    if matches:
                        x = matches[0]['coordinates']['x']
                        y = matches[0]['coordinates']['y']
                        transformed_file.write(f"{x},{y},Residential\n")
                    else:
                        print(f"Warning: No geocode match for {address}")
                except requests.RequestException as e:
                    print(f"Error during geocoding: {e}")

    print("Transformation complete. Data saved to new_addresses.csv")


def load():
    print("Loading data into GIS...")

    # Set environment settings
    arcpy.env.workspace = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak\WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

    # Set local variables
    in_table = r"C:\Users\Spencer\Downloads\new_addresses.csv"
    out_feature_class = "avoid_points"
    x_coords = "X"
    y_coords = "Y"

    # Make the XY event layer
    try:
        arcpy.management.XYTableToPoints(in_table, out_feature_class, x_coords, y_coords)
        print(f"Feature class '{out_feature_class}' created successfully.")
        print(f"Total rows loaded: {arcpy.GetCount_management(out_feature_class)}")
    except arcpy.ExecuteError:
        print("Error during GIS data load:", arcpy.GetMessages(2))


if __name__ == "__main__":
    extract()
    transform()
    load()
