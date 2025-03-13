import arcpy


def intersect(layer_list, input_lyr_name):
    """Runs an intersect analysis between the given buffer layers."""
    arcpy.Intersect_analysis(layer_list, input_lyr_name)


def buffer_layer(input_gdb, input_layer, dist):
    """Runs a buffer analysis on the input_layer with a user-specified distance."""

    # Ensure distance is formatted correctly
    dist = f"{dist} miles"

    # Output layer will be named input layer + "_buf"
    output_layer = fr"{input_gdb}{input_layer}_buf"

    # Buffer operation with parameters FULL, ROUND, ALL
    buf_layer = fr"{input_gdb}{input_layer}"
    arcpy.Buffer_analysis(buf_layer, output_layer, dist, "FULL", "ROUND", "ALL")

    return output_layer


def main():
    """Main function to execute buffer and intersect operations."""

    # Define workspace and enable overwrite
    arcpy.env.workspace = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Assignments\Assignment1b\Assignment1b.gdb"
    arcpy.env.overwriteOutput = True

    # Input geodatabase
    input_gdb = r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Assignments\Assignment1b\Assignment1b.gdb\USA\\"

    # Get distances from parameters
    dist_cities = arcpy.GetParameterAsText(0)
    dist_rivers = arcpy.GetParameterAsText(1)
    intersect_lyr_name = arcpy.GetParameterAsText(2)  # Get intersect output layer name

    # Buffer cities
    buf_cities = buffer_layer(input_gdb, "cities", dist_cities)
    print(f"Buffer layer {buf_cities} created.")

    # Buffer rivers
    buf_rivers = buffer_layer(input_gdb, "us_rivers", dist_rivers)
    print(f"Buffer layer {buf_rivers} created.")

    # Perform intersection
    lyr_list = [buf_rivers, buf_cities]
    intersect(lyr_list, intersect_lyr_name)
    print(f"New intersect layer generated: {intersect_lyr_name}")

    # Open ArcGIS project and add the new layer
    aprx = arcpy.mp.ArcGISProject(
        r"C:\Users\Spencer\Desktop\FRCCSpring2025\ProgrammingGIS\Assignments\Assignment1b\Assignment1b.aprx"
    )
    map_doc = aprx.listMaps()[0]
    map_doc.addDataFromPath(fr"{arcpy.env.workspace}\{intersect_lyr_name}")

    # Save the project
    aprx.save()


if __name__ == '__main__':
    main()
