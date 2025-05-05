import requests
from etl.SpatialEtl import SpatialEtl
import csv
import arcpy

class GSheetsEtl(SpatialEtl):
    """
    ETL (Extract, Transform, Load) class for handling address data collected from a Google Form
    (via a published Google Sheets URL).

    This class extends SpatialEtl and adds specific implementations to:
    - Extract address data from a remote CSV (Google Sheets published as CSV)
    - Geocode the addresses using the U.S. Census Bureau Geocoding API
    - Load the geocoded points into a GIS as a point feature class
    """

    # A dictionary to hold configuration keys and values
    config_dict = None

    def __init__(self, config_dict):
        """
        Initialize the GSheetsEtl instance with a given configuration dictionary.

        Parameters:
        - config_dict (dict): Dictionary containing configuration options such as:
            - 'remote_url': URL to the published Google Sheets CSV
            - 'proj_dir': Local project directory path for saving and loading files
        """
        super().__init__(config_dict)

    def extract(self):
        """
        Extract the address data from the published Google Sheets CSV URL.

        Downloads the CSV content from the remote URL and saves it as 'addresses.csv'
        in the local project directory.
        """
        print("Extracting addresses from google form spreadsheet")
        r = requests.get(self.config_dict.get('remote_url'))
        r.encoding = "utf-8"
        data = r.text
        with open(f"{self.config_dict.get('proj_dir')}addresses.csv", "w") as output_file:
            output_file.write(data)

    def transform(self):
        """
        Transform the extracted address data by:
        - Appending 'Boulder CO' to each street address
        - Geocoding each address using the U.S. Census Geocoding API
        - Writing the resulting X, Y coordinates and address type to 'new_addresses.csv'

        Any addresses without a successful geocode match are logged with a warning.
        """
        print("Adding City, State and Geocoding addresses...")

        input_file = f"{self.config_dict.get('proj_dir')}addresses.csv"
        output_file = f"{self.config_dict.get('proj_dir')}new_addresses.csv"

        with open(output_file, "w", newline="", encoding="utf-8") as transformed_file:
            transformed_file.write("X,Y,Type\n")

            with open(input_file, "r", encoding="utf-8") as partial_file:
                csv_dict = csv.DictReader(partial_file, delimiter=',')

                for row in csv_dict:
                    address = f"{row['Street Address']} Boulder CO"
                    print(f"Geocoding: {address}")

                    geocode_url = (
                        f"https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
                        f"?address={address}&benchmark=2020&format=json"
                    )

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
        """
        Load the transformed geocoded data into a GIS.

        Creates a point feature class 'avoid_points' from the 'new_addresses.csv' file
        using the X and Y coordinates.
        """
        print("Loading data into GIS...")

        # Set local variables
        in_table = f"{self.config_dict.get('proj_dir')}new_addresses.csv"
        out_feature_class = "avoid_points"
        x_coords = "X"
        y_coords = "Y"

        # Make the XY event layer
        arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

        # Print the total number of loaded points
        print(arcpy.GetCount_management(out_feature_class))

    def process(self):
        """
        Orchestrate the full ETL workflow:
        - Extract address data from Google Sheets
        - Transform and geocode the addresses
        - Load the geocoded points into a GIS feature class
        """
        self.extract()
        self.transform()
        self.load()
