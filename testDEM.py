#rewriting dem automation tools

#setting up path 
import arcpy
import os

arcpy.env.overwriteOutput = True
    
    
#makign diction so can sawp out the path for other counties.

config ={
    
    "input_folder" : r"C:\ArcPyProjects\test\inputs",
    "output_folder" : r"C:\ArcPyProjects\test\outputs",
    "crs" : arcpy.SpatialReference(6543),
    "gdb" : "final.gdb",
    "clip_feature" : r"C:\ArcPyProjects\demTo\inputs\Wake_Boundary.shp"
}

                            

def setup_gdb(output_folder, gdb):

    gdb_path = os.path.join(output_folder, gdb)
    if not arcpy.Exists(gdb_path):
        arcpy.CreateFileGDB_management(output_folder, gdb)
        print(f"GDB created: {gdb_path}")
    else:
        print("GDB already exists")

    return gdb_path


#Detecting tiles
def detect_tiles(input_folder):
    try:

        tileList = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".tif")]
        print(f"Tiles detected: {[os.path.basename(t) for t in tileList]}")

    except Exception as e:
            print(f"Error detecting tiles: {e}  ")
    return tileList



def mosaic_tiles(tileList, gdb_path):
    try:
        mosaic_tiff = arcpy.management.MosaicToNewRaster(
            input_rasters = tileList,
            output_location= gdb_path,
            coordinate_system_for_the_raster= "#",
            pixel_type="32_BIT_FLOAT",
            raster_dataset_name_with_extension="mosaiced_tiles",
            number_of_bands=1,
            mosaic_method="BLEND"
        )

    except Exception as e:
        print(f"Error during processing: {e}  ")

    return mosaic_tiff

def Cliped_mosaic(mosaic_tiff, gdb_path, clip_feature):
    try:
        clipped_raster = arcpy.management.Clip(
            in_raster= mosaic_tiff,
            out_raster= os.path.join(gdb_path, "clipped_mosaic"),
            in_template_dataset= clip_feature
            
            )

    except Exception as e:
        print(f"Error clipping mosaic: {e}")
    return clipped_raster

def Project_cliped_mosaic(Clipped_mosaic, gdb_path, crs):

    projected_clipped_raster = arcpy.management.ProjectRaster(
        in_raster= Clipped_mosaic,
        out_raster=os.path.join(gdb_path, "projected_clipped_mosaic"),
        out_coor_system=crs
    )

    return projected_clipped_raster

    

def main():
    

    gdb_path =setup_gdb(config["output_folder"], config["gdb"])

    tilesList = detect_tiles(config["input_folder"])

    res_mosaic_tiles = mosaic_tiles(tilesList, gdb_path)

    cliped_mosaic = Cliped_mosaic(res_mosaic_tiles, gdb_path, config["clip_feature"] )

    finalProjectedMosaic = os.path.join(gdb_path, "Final_Projected_clipped_mosaic")
    
    finalProjectedMosaic = Project_cliped_mosaic(cliped_mosaic, gdb_path, config["crs"])
    
    print(f"Processing complete.{finalProjectedMosaic}")



#do map algebra here




    

if __name__ == "__main__":
    main()