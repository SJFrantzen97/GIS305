import yaml
import arcpy
import arcpy.mp  # Required for adding data to ArcGIS Pro
import logging
from etl.GSheetsEtl import GSheetsEtl


def setup():
    arcpy.env.parallelProcessingFactor = "100%"
    logging.debug("Entering setup()")
    arcpy.env.workspace = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak\WestNileOutbreak.gdb"
    arcpy.env.overwriteOutput = True
    with open('config/wnvoutbreak.yaml') as f:
        config_dict = yaml.load(f, Loader=yaml.FullLoader)

    logging.basicConfig(
        filename=f"{config_dict.get('proj_dir')}wnv.log",
        filemode="w",
        level=logging.DEBUG
    )
    logging.debug("Exiting setup()")
    return config_dict


def etl():
    logging.debug("Entering etl()")
    logging.info("Start ETL process...")
    etl_instance = GSheetsEtl(config_dict)
    etl_instance.process()
    logging.debug("Exiting etl()")


def buffer(layer_name, buff_dist):
    logging.debug(f"Entering buffer() with layer_name={layer_name}, buff_dist={buff_dist}")
    output_buffer_layer_name = f"buf_{layer_name}"
    logging.info(f"Buffering {layer_name} to generate {output_buffer_layer_name}")

    arcpy.analysis.Buffer(layer_name, output_buffer_layer_name, buff_dist)
    logging.debug("Exiting buffer()")


def intersect():
    logging.debug("Entering intersect()")
    output_layer = input("Enter a name for the intersect output layer: ").strip()
    output_layer = output_layer.replace(" ", "_")[:50]

    buffer_layers = ["buf_Mosquito_Larval_Sites", "buf_Wetlands"]
    buffer_layers2 = ["buf_Lakes_and_Reservoirs", "buf_OSMP_Properties"]
    logging.info(f"Performing intersect on: {buffer_layers}")
    logging.info(f"Performing intersect on: {buffer_layers2}")

    try:
        existing_layers = [layer for layer in buffer_layers if arcpy.Exists(layer)]
        if not existing_layers:
            logging.error("No buffer layers exist! Cannot perform intersection.")
            return None

        arcpy.analysis.Intersect(existing_layers, output_layer, "ALL")
        logging.info(f"Intersect operation successful! Output saved as {output_layer}")

        if arcpy.Exists(output_layer):
            logging.info(f"Verified: {output_layer} exists.")
            return output_layer
        else:
            logging.error(f"{output_layer} was not created.")
            return None

    except Exception as e:
        logging.error(f"Error during intersect: {e}")
        return None
    finally:
        logging.debug("Exiting intersect()")


def erase_analysis(input_layer, erase_layer, output_layer):
    logging.debug(f"Entering erase_analysis() with input_layer={input_layer}, erase_layer={erase_layer}, output_layer={output_layer}")

    try:
        if not arcpy.Exists(input_layer) or not arcpy.Exists(erase_layer):
            logging.error("Input or erase layer does not exist. Cannot perform erase.")
            return None

        arcpy.analysis.Erase(in_features=input_layer, erase_features=erase_layer, out_feature_class=output_layer)
        logging.info(f"Erase operation successful! Output saved as {output_layer}")

        if arcpy.Exists(output_layer):
            logging.info(f"Verified: {output_layer} exists.")
            return output_layer
        else:
            logging.error(f"{output_layer} was not created.")
            return None

    except Exception as e:
        logging.error(f"Error during erase analysis: {e}")
        return None
    finally:
        logging.debug("Exiting erase_analysis()")


def spatial_join(address_layer, intersect_layer):
    logging.debug(f"Entering spatial_join() with address_layer={address_layer}, intersect_layer={intersect_layer}")

    if not intersect_layer or not arcpy.Exists(intersect_layer):
        logging.error(f"No valid intersect layer provided ({intersect_layer}). Skipping spatial join.")
        return None

    output_joined_layer = "Joined_Addresses"

    try:
        logging.info(f"Performing spatial join between {address_layer} and {intersect_layer}...")

        arcpy.analysis.SpatialJoin(
            target_features=address_layer,
            join_features=intersect_layer,
            out_feature_class=output_joined_layer,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL"
        )

        logging.info(f"Spatial join successful! Output saved as {output_joined_layer}")

        if arcpy.Exists(output_joined_layer):
            logging.info(f"Verified: {output_joined_layer} exists.")
        else:
            logging.error(f"{output_joined_layer} was not created.")
            return None

        return output_joined_layer

    except Exception as e:
        logging.error(f"Error during spatial join: {e}")
        return None
    finally:
        logging.debug("Exiting spatial_join()")


def add_layer_to_map(layer_name):
    logging.debug(f"Entering add_layer_to_map() with layer_name={layer_name}")

    try:
        proj_path = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\WestNileOutbreak.aprx")

        map_doc = aprx.listMaps()[0]
        full_layer_path = f"{arcpy.env.workspace}\\{layer_name}"

        if not arcpy.Exists(full_layer_path):
            logging.error(f"{full_layer_path} does not exist, skipping.")
            return

        map_doc.addDataFromPath(full_layer_path)
        aprx.save()

        logging.info(f"Successfully added {layer_name} to the ArcGIS Pro map!")

    except Exception as e:
        logging.error(f"Error adding {layer_name} to map: {e}")
    finally:
        logging.debug("Exiting add_layer_to_map()")


def exportMap():
    logging.debug("Entering exportMap()")
    try:
        aprx = arcpy.mp.ArcGISProject(f"{config_dict.get('proj_dir')}WestNileOutbreak.aprx")
        lyt = aprx.listLayouts()[0]

        subtitle = input("Enter the subtitle for the map: ").strip()

        for el in lyt.listElements():
            if "Title" in el.name:
                el.text = el.text + f" {subtitle}"

        pdf_path = f"{config_dict.get('proj_dir')}WestNileOutbreak_Map.pdf"
        lyt.exportToPDF(pdf_path)
        logging.info(f"Map exported successfully as {pdf_path}")

    except Exception as e:
        logging.error(f"Error during map export: {e}")
    finally:
        logging.debug("Exiting exportMap()")


# Run the script
if __name__ == '__main__':
    logging.debug("Entering main script block")
    config_dict = setup()
    logging.info("Starting West Nile Virus Simulation")
    logging.info(config_dict)

    etl()

    buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]
    for layer in buffer_layer_list:
        buffer(layer, "1500 feet")

    # Buffer avoid_points layer
    buffer("avoid_points", "1500 feet")

    intersect_layer = intersect()
    if intersect_layer:
        add_layer_to_map(intersect_layer)

        # Perform erase analysis
        erased_layer = erase_analysis(intersect_layer, "buf_avoid_points", "Erased_Intersect")
        if erased_layer:
            add_layer_to_map(erased_layer)

            # Perform spatial join on the erased result
            joined_layer = spatial_join("Addresses", erased_layer)
            if joined_layer:
                add_layer_to_map(joined_layer)

    exportMap()

    logging.debug("Exiting main script block")
