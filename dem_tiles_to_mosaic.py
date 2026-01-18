import arcpy
import os
from arcpy.sa import *

# =========================================================================
# USER CONFIGURATION: PLUG YOUR PATHS HERE
# =========================================================================
input_folder = r"C:\ArcPyProjects\demTo\inputs"
output_workspace = r"C:\ArcPyProjects\demTo\outputs"
in_clip_feature = r"C:\ArcPyProjects\demTo\inputs\Wake_Boundary.shp"
output_crs = arcpy.SpatialReference(3359) # NAD 1983 StatePlane NC Meters

# Threshold for Map Algebra (Meters)
# Areas higher than this will be coded as 1, lower as 0.
elevation_threshold = 120 
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
    final_dem = os.path.join(output_workspace, "Wake_DEM_Final_Clipped.tif")
    arcpy.management.Clip(
        in_raster=projected_raster, 
        out_raster=final_dem, 
        in_template_dataset=in_clip_feature,
        nodata_value="-9999",
        clipping_geometry="ClippingGeometry", 
        maintain_clipping_extent="NO_MAINTAIN_EXTENT" 
    )

    # 5. MAP ALGEBRA ANALYSIS (Identify High Ground)
    # Using the Raster() class from Lesson 1.6.3
    print(f"Step 5: Identifying terrain above {elevation_threshold}m...")
    
    # This single line of math creates the analysis
    high_ground_bool = Raster(final_dem) > elevation_threshold
    
    # Save the analysis result permanently to disk
    analysis_output = os.path.join(output_workspace, "Wake_High_Ground_Analysis.tif")
    high_ground_bool.save(analysis_output)

    print(f"--- SUCCESS ---")
    print(f"Final DEM: {final_dem}")
    print(f"Analysis Result: {analysis_output}")

except Exception as e:
    print(f"Automation failed: {e}")

finally:
    # Always check the license back in
    arcpy.CheckInExtension("Spatial")