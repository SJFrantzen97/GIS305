import yaml
import arcpy
import arcpy.mp  # Required for adding data to ArcGIS Pro
from etl.GSheetsEtl import GSheetsEtl

def setup():
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)
    return config_dict

def etl():
    print("Start etl process...")
    etl_instance = GSheetsEtl("https://foo_bar.com", "C/Users", "GSheets", "C:/Users/my.gdb")
    etl_instance.process()

def setup():
    arcpy.env.workspace = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak\WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True

def buffer(layer_name, buff_dist):
    output_buffer_layer_name = f"buf_{layer_name}"
    print(f"Buffering {layer_name} to generate {output_buffer_layer_name}")

    arcpy.analysis.Buffer(layer_name, output_buffer_layer_name, buff_dist)

def intersect():
    output_layer = input("Enter a name for the intersect output layer: ").strip()
    output_layer = output_layer.replace(" ", "_")[:50]  # Ensure a valid name

    buffer_layers = ["buf_Mosquito_Larval_Sites", "buf_Wetlands", "buf_Lakes_and_Reservoirs", "buf_OSMP_Properties"]

    print(f"Performing intersect on: {buffer_layers}")

    try:
        existing_layers = [layer for layer in buffer_layers if arcpy.Exists(layer)]
        if not existing_layers:
            print("❌ Error: No buffer layers exist! Cannot perform intersection.")
            return None

        arcpy.analysis.Intersect(existing_layers, output_layer, "ALL")
        print(f"✅ Intersect operation successful! Output saved as {output_layer}")

        if arcpy.Exists(output_layer):
            print(f"✅ Verified: {output_layer} exists.")
            return output_layer
        else:
            print(f"❌ Error: {output_layer} was not created.")
            return None

    except Exception as e:
        print(f"❌ Error during intersect: {e}")
        return None  # Return None if an error occurs

def spatial_join(address_layer, intersect_layer):
    if not intersect_layer or not arcpy.Exists(intersect_layer):
        print(f"❌ Error: No valid intersect layer provided ({intersect_layer}). Skipping spatial join.")
        return None

    output_joined_layer = "Joined_Addresses"

    try:
        print(f"Performing spatial join between {address_layer} and {intersect_layer}...")

        arcpy.analysis.SpatialJoin(
            target_features=address_layer,
            join_features=intersect_layer,
            out_feature_class=output_joined_layer,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL"
        )

        print(f"✅ Spatial join successful! Output saved as {output_joined_layer}")

        if arcpy.Exists(output_joined_layer):
            print(f"✅ Verified: {output_joined_layer} exists.")
        else:
            print(f"❌ Error: {output_joined_layer} was not created.")
            return None

        return output_joined_layer

    except Exception as e:
        print(f"❌ Error during spatial join: {e}")
        return None

def add_layer_to_map(layer_name):
    """Adds the specified feature class to the ArcGIS Pro project."""
    try:
        proj_path = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\WestNileOutbreak.aprx")  # Open the ArcGIS Pro project

        map_doc = aprx.listMaps()[0]  # Get the first map in the project
        full_layer_path = f"{arcpy.env.workspace}\\{layer_name}"  # Construct full layer path

        if not arcpy.Exists(full_layer_path):
            print(f"❌ Error: {full_layer_path} does not exist, skipping.")
            return

        map_doc.addDataFromPath(full_layer_path)  # Add the feature class to the map
        aprx.save()  # Save the project

        print(f"✅ Successfully added {layer_name} to the ArcGIS Pro map!")

    except Exception as e:
        print(f"❌ Error adding {layer_name} to map: {e}")

# Run the script
if __name__ == '__main__':
    setup()

    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]
    for layer in buffer_layer_list:
        buffer(layer, "0.95 miles")

    intersect_layer = intersect()  # Capture the intersect output name
    if intersect_layer:
        add_layer_to_map(intersect_layer)  # Add intersect output to ArcGIS Pro

    joined_layer = spatial_join("Addresses", intersect_layer)  # Pass the correct intersect layer name
    if joined_layer:
        add_layer_to_map(joined_layer)  # Add joined output to ArcGIS Pro


