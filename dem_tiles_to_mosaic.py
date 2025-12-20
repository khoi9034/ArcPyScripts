import arcpy
import os
from arcpy.sa import *

# =========================================================================
# USER CONFIGURATION: PLUG YOUR PATHS HERE
# =========================================================================
# Folder containing your raw .tif tiles (e.g., SRTM tiles)
input_folder = r"C:\ArcPyProjects\demTo\inputs"

# Folder where you want all your results saved
output_workspace = r"C:\ArcPyProjects\demTo\outputs"

# Path to your boundary shapefile (The Cookie Cutter)
in_clip_feature = r"C:\ArcPyProjects\demTo\inputs\Wake_Boundary.shp"

# Output Coordinate System (WKID 3359 = NAD 1983 StatePlane NC Meters)
output_crs = arcpy.SpatialReference(3359)
# =========================================================================

# Environment Settings
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")

try:
    # 1. AUTOMATED TILE DISCOVERY
    print("Step 1: Discovering tiles...")
    tile_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".tif")]
    print(f"Found {len(tile_list)} tiles.")

    # 2. MOSAICING (Keeping Native Units)
    print("Step 2: Mosaicking tiles into seamless sheet...")
    raw_mosaic = os.path.join(output_workspace, "temp_raw_mosaic.tif")
    arcpy.management.MosaicToNewRaster(
        input_rasters=tile_list,
        output_location=output_workspace,
        raster_dataset_name_with_extension="temp_raw_mosaic.tif",
        coordinate_system_for_the_raster="#",
        pixel_type="16_BIT_SIGNED",
        number_of_bands=1,
        mosaic_method="FIRST"
    )

    # 3. PROJECTING (Sphere to Flat Map)
    print("Step 3: Warping data from Degrees to Meters...")
    projected_raster = os.path.join(output_workspace, "srtm_projected_meters.tif")
    arcpy.management.ProjectRaster(
        in_raster=raw_mosaic,
        out_raster=projected_raster,
        out_coor_system=output_crs,
        resampling_type="BILINEAR",
        cell_size="30"
    )

    # 4. PRECISION CLIPPING (The Shape-Based Cut)
    print("Step 4: Clipping to irregular polygon boundary...")
    final_output = os.path.join(output_workspace, "Wake_DEM_Final_Clipped.tif")
    arcpy.management.Clip(
        in_raster=projected_raster, 
        out_raster=final_output, 
        in_template_dataset=in_clip_feature,
        nodata_value="-9999",
        clipping_geometry="ClippingGeometry", 
        maintain_clipping_extent="NO_MAINTAIN_EXTENT" 
    )

    arcpy.management.CalculateStatistics(final_output)
    print(f"COMPLETE! Final file ready at: {final_output}")

except Exception as e:
    print(f"Automation failed: {e}")

finally:
    arcpy.CheckInExtension("Spatial")