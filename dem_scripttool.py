import arcpy
import os
from arcpy.sa import *

# --- 1. Get Parameters from the Tool Interface ---
# The index (0, 1, 2...) corresponds to the order in the Tool Properties
input_folder      = arcpy.GetParameterAsText(0) # Input Tiles Folder
output_workspace  = arcpy.GetParameterAsText(1) # Destination Folder
in_clip_feature   = arcpy.GetParameterAsText(2) # Boundary Polygon
output_crs        = arcpy.GetParameter(3)       # Spatial Reference Object
elev_threshold    = float(arcpy.GetParameterAsText(4)) # Math Threshold

# Environment Settings
arcpy.env.overwriteOutput = True
arcpy.CheckOutExtension("Spatial")

try:
    # --- 2. Automated Tile Discovery ---
    arcpy.AddMessage("Step 1: Discovering .tif tiles...")
    tile_list = [os.path.join(input_folder, f) for f in os.listdir(input_folder) if f.endswith(".tif")]
    
    if not tile_list:
        arcpy.AddError("No tiles found! Check your input folder path.")
        raise Exception("Input folder empty.")

    # --- 3. Mosaic Step ---
    arcpy.AddMessage("Step 2: Creating seamless Mosaic...")
    temp_mosaic = os.path.join(output_workspace, "temp_mosaic.tif")
    arcpy.management.MosaicToNewRaster(
        input_rasters=tile_list,
        output_location=output_workspace,
        raster_dataset_name_with_extension="temp_mosaic.tif",
        coordinate_system_for_the_raster="#", # Preserves native degrees
        pixel_type="16_BIT_SIGNED",
        number_of_bands=1,
        mosaic_method="FIRST"
    )

    # --- 4. Projection Step ---
    arcpy.AddMessage(f"Step 3: Projecting to {output_crs.name}...")
    projected_raster = os.path.join(output_workspace, "projected_dem.tif")
    arcpy.management.ProjectRaster(
        in_raster=temp_mosaic, 
        out_raster=projected_raster, 
        out_coor_system=output_crs, 
        resampling_type="BILINEAR", 
        cell_size="30"
    )

    # --- 5. Precision Clip ---
    arcpy.AddMessage("Step 4: Clipping to irregular boundary shape...")
    final_dem = os.path.join(output_workspace, "Final_Clipped_DEM.tif")
    arcpy.management.Clip(
        in_raster=projected_raster, 
        out_raster=final_dem, 
        in_template_dataset=in_clip_feature,
        nodata_value="-9999",
        clipping_geometry="ClippingGeometry", 
        maintain_clipping_extent="NO_MAINTAIN_EXTENT" 
    )

    # --- 6. Map Algebra Analysis ---
    arcpy.AddMessage(f"Step 5: Extracting terrain above {elev_threshold} meters...")
    analysis_output = os.path.join(output_workspace, "Terrain_Suitability_Analysis.tif")
    
    # Mathematical expression using the Raster class
    suitability = Raster(final_dem) > elev_threshold
    suitability.save(analysis_output)

    arcpy.AddMessage("---------------------------------------")
    arcpy.AddMessage("SUCCESS: Pipeline complete.")
    arcpy.AddMessage(f"Final Products saved to: {output_workspace}")

except Exception as e:
    arcpy.AddError(f"CRITICAL ERROR: {str(e)}")

finally:
    arcpy.CheckInExtension("Spatial")