import yaml
import arcpy
import arcpy.mp
import logging
from etl.GSheetsEtl import GSheetsEtl


def setup():
    """
       Sets up the ArcGIS Pro project environment and initializes configuration settings.

       Configures environment variables, logging, map spatial reference, and loads the YAML configuration file.

       Returns:
           dict: A dictionary containing configuration values loaded from the YAML file, or None if an error occurs.
       """
    logging.debug("Entering setup()")
    try:
        arcpy.env.parallelProcessingFactor = "100%"
        arcpy.env.workspace = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak\WestNileOutbreak.gdb"
        arcpy.env.overwriteOutput = True

        with open('config/wnvoutbreak.yaml') as f:
            config_dict = yaml.load(f, Loader=yaml.FullLoader)

        logging.basicConfig(
            filename=f"{config_dict.get('proj_dir')}wnv.log",
            filemode="w",
            level=logging.DEBUG
        )

        proj_path = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\WestNileOutbreak.aprx")
        map_doc = aprx.listMaps()[0]

        spatial_ref = arcpy.SpatialReference(26953)
        map_doc.defaultCamera.spatialReference = spatial_ref
        aprx.save()

        logging.info("Set map document spatial reference to NAD 1983 StatePlane Colorado North (US Feet).")

        return config_dict

    except Exception as e:
        logging.error(f"Error in setup(): {e}")
        return None
    finally:
        logging.debug("Exiting setup()")


def etl():
    """
        Runs the ETL (Extract, Transform, Load) process using the GSheetsEtl class.

        Instantiates the ETL process with the global config_dict and executes its process method.
        """
    logging.debug("Entering etl()")
    try:
        logging.info("Start ETL process...")
        etl_instance = GSheetsEtl(config_dict)
        etl_instance.process()
    except Exception as e:
        logging.error(f"Error in etl(): {e}")
    finally:
        logging.debug("Exiting etl()")


def buffer(layer_name, buff_dist):
    """
        Creates a buffer around the specified layer.

        Args:
            layer_name (str): The name of the input feature layer to buffer.
            buff_dist (str): The buffer distance (e.g. "1500 feet").

        Returns:
            None
        """
    logging.debug(f"Entering buffer() with layer_name={layer_name}, buff_dist={buff_dist}")
    try:
        output_buffer_layer_name = f"buf_{layer_name}"
        logging.info(f"Buffering {layer_name} to generate {output_buffer_layer_name}")
        arcpy.analysis.Buffer(layer_name, output_buffer_layer_name, buff_dist)
    except Exception as e:
        logging.error(f"Error in buffer(): {e}")
    finally:
        logging.debug("Exiting buffer()")


def intersect():
    """
        Performs an intersection analysis between buffer layers.

        Asks the user for an output layer name, intersects specified buffer layers, and verifies the output.

        Returns:
            str: The name of the intersect output layer, or None if an error occurs.
        """
    logging.debug("Entering intersect()")
    try:
        output_layer = input("Enter a name for the intersect output layer: ").strip().replace(" ", "_")[:50]
        buffer_layers = ["buf_Mosquito_Larval_Sites", "buf_Wetlands"]
        logging.info(f"Performing intersect on: {buffer_layers}")

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
        logging.error(f"Error in intersect(): {e}")
        return None
    finally:
        logging.debug("Exiting intersect()")


def erase_analysis(input_layer, erase_layer, output_layer):
    """
        Erases areas of the input layer using the erase layer.

        Args:
            input_layer (str): The name of the feature layer to be erased.
            erase_layer (str): The name of the feature layer used to erase from the input layer.
            output_layer (str): The name of the output feature class.

        Returns:
            str: The name of the output layer, or None if an error occurs.
        """
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
        logging.error(f"Error in erase_analysis(): {e}")
        return None
    finally:
        logging.debug("Exiting erase_analysis()")


def spatial_join(address_layer, intersect_layer):
    """
        Performs a spatial join between the address and intersect layers.

        Args:
            address_layer (str): The name of the address feature layer.
            intersect_layer (str): The name of the intersect feature layer.

        Returns:
            str: The name of the joined output layer, or None if an error occurs.
        """
    logging.debug(f"Entering spatial_join() with address_layer={address_layer}, intersect_layer={intersect_layer}")
    try:
        if not intersect_layer or not arcpy.Exists(intersect_layer):
            logging.error(f"No valid intersect layer provided ({intersect_layer}). Skipping spatial join.")
            return None

        output_joined_layer = "Joined_Addresses"
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
            return output_joined_layer
        else:
            logging.error(f"{output_joined_layer} was not created.")
            return None

    except Exception as e:
        logging.error(f"Error in spatial_join(): {e}")
        return None
    finally:
        logging.debug("Exiting spatial_join()")


def spatial_join_and_filter(address_layer, analysis_layer, output_layer):
    """
        Performs a spatial join between address and analysis layers, adds the result to the map,
        and applies a definition query to filter for relevant records.

        Args:
            address_layer (str): The name of the address feature layer.
            analysis_layer (str): The name of the analysis feature layer.
            output_layer (str): The name of the resulting joined output feature class.

        Returns:
            str: The name of the output layer, or None if an error occurs.
        """
    logging.debug(f"Entering spatial_join_and_filter() with address_layer={address_layer}, analysis_layer={analysis_layer}, output_layer={output_layer}")
    try:
        if not arcpy.Exists(address_layer) or not arcpy.Exists(analysis_layer):
            logging.error(f"One or both layers don't exist: {address_layer}, {analysis_layer}")
            return None

        arcpy.analysis.SpatialJoin(
            target_features=address_layer,
            join_features=analysis_layer,
            out_feature_class=output_layer,
            join_operation="JOIN_ONE_TO_ONE",
            join_type="KEEP_ALL"
        )
        logging.info(f"Spatial Join completed. Output: {output_layer}")

        proj_path = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\WestNileOutbreak.aprx")
        map_doc = aprx.listMaps()[0]

        full_output_path = f"{arcpy.env.workspace}\\{output_layer}"
        map_doc.addDataFromPath(full_output_path)

        for lyr in map_doc.listLayers():
            if lyr.name == output_layer:
                lyr.definitionQuery = "Join_Count = 1"
                logging.info(f"Definition query applied to {output_layer}: Join_Count = 1")
                break

        aprx.save()
        return output_layer

    except Exception as e:
        logging.error(f"Error in spatial_join_and_filter(): {e}")
        return None
    finally:
        logging.debug("Exiting spatial_join_and_filter()")


def add_layer_to_map(layer_name):
    """
       Adds a specified feature layer to the current ArcGIS Pro map document.

       Args:
           layer_name (str): The name of the feature layer to add.

       Returns:
           None
       """
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
        logging.error(f"Error in add_layer_to_map(): {e}")
    finally:
        logging.debug("Exiting add_layer_to_map()")


def apply_simple_renderer(layer_name):
    """
      Applies a simple renderer with a custom symbology to a feature layer in the map.

      Args:
          layer_name (str): The name of the feature layer to symbolize.

      Returns:
          None
      """
    logging.debug(f"Entering apply_simple_renderer() with layer_name={layer_name}")
    try:
        proj_path = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Labs\Lab1\WestNileOutbreak"
        aprx = arcpy.mp.ArcGISProject(rf"{proj_path}\WestNileOutbreak.aprx")
        map_doc = aprx.listMaps()[0]

        target_layer = None
        for lyr in map_doc.listLayers():
            if lyr.name == layer_name:
                target_layer = lyr
                break

        if not target_layer:
            logging.error(f"Layer {layer_name} not found in the map.")
            return

        sym = target_layer.symbology
        sym.updateRenderer('SimpleRenderer')
        symbol = sym.renderer.symbol
        symbol.color = {'RGB': [255, 0, 0, 50]}
        symbol.outlineColor = {'RGB': [0, 0, 0, 100]}
        symbol.outlineWidth = 1.0

        target_layer.symbology = sym
        aprx.save()
        logging.info(f"Applied simple renderer to {layer_name}.")

    except Exception as e:
        logging.error(f"Error in apply_simple_renderer(): {e}")
    finally:
        logging.debug("Exiting apply_simple_renderer()")


def exportMap():
    """
        Exports the current ArcGIS Pro map layout to a PDF file with a user-provided subtitle.

        Prompts the user for a subtitle, updates the layout title, and exports the map as a PDF.

        Returns:
            None
        """
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
        logging.error(f"Error in exportMap(): {e}")
    finally:
        logging.debug("Exiting exportMap()")


if __name__ == '__main__':
    logging.debug("Entering main script block")
    config_dict = setup()
    if config_dict:
        logging.info("Starting West Nile Virus Simulation")
        logging.info(config_dict)

        etl()

        buffer_layer_list = ["Mosquito_Larval_Sites", "Wetlands", "Lakes_and_Reservoirs", "OSMP_Properties"]
        for layer in buffer_layer_list:
            buffer(layer, "1500 feet")

        buffer("avoid_points", "1500 feet")

        intersect_layer = intersect()
        if intersect_layer:
            add_layer_to_map(intersect_layer)

            erased_layer = erase_analysis(intersect_layer, "buf_avoid_points", "erased_intersect")
            if erased_layer:
                add_layer_to_map(erased_layer)
                apply_simple_renderer("erased_intersect")

                target_layer = spatial_join_and_filter("Addresses", erased_layer, "target_addresses")
                if target_layer:
                    add_layer_to_map(target_layer)

        exportMap()

    logging.debug("Exiting main script block")



