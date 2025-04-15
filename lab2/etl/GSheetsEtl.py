import requests
from SpatialEtl import SpatialEtl


class GSheetsEtl(SpatialEtl):

    #A dictionary of configuration keys and values
    config_dict = None

    def __init__(self, config_dict):
        super().__init__(config_dict)

    def extract(self):
        print("Extracting addresses from google form spreadsheet")
        r = requests.get(self.config_dict.get('remote_url'))
        r.encoding = "utf-8"
        data = r.text
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "w") as output_file:
            output_file.write(data)

    def transform(self):
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

    def load(self):
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
            arcpy.management.XYTableToPoints(in_table, out_feature_class, x_coords, y_coords)

            # Print the total rows
            print(arcpy.GetCount_management(out_feature_class))

    def process(self):
        self().extract()
        self().transform()
        self().load()