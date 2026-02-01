# ==========================================================================================
# DESCRIPTION: 
#   This script automates common County GIS tasks: batch buffering, projection 
#   standardization, and data validation. It includes a custom logging system to 
#   track Quality Control (QC) results and geometry repairs.
#
# KEY CONCEPTS DEMONSTRATED:
#   1. Data Validation: Checking coordinate systems and file existence.
#   2. Automation: Using dictionaries to process multiple layers in a single loop.
#   3. Topology & QC: Using 'Dissolve' for clean results and 'Repair Geometry' for data health.
#   4. Error Handling: Try/Except blocks with logging for "fail-safe" operations.
# ==========================================================================================

import arcpy
import os
import datetime

# -------------------------------------------------------------------------
# 1. SETUP & CONFIGURATION
# -------------------------------------------------------------------------

# NC State Plane Feet (2264) is the legal standard for measuring distance in NC Counties.
# Using the wrong system can result in buffers that are mathematically incorrect.
county_standard = arcpy.SpatialReference(2264) 

# Paths to our database and our audit log (the "paper trail" for our work)
workspace = r"C:\CountyProjects\Zoning_Compliance.gdb"
log_file = r"C:\CountyProjects\Outputs\Daily_Log.txt"

# BATCH CONFIGURATION:
# This dictionary maps the Layer Name to the required Buffer Distance.
# This makes the script "scalable"—you can add 50 layers here and it will process them all.
tasks = {
    "Schools": "1000 Feet",
    "Hydrants": "500 Feet",
    "Parks": "300 Feet"
}

# Environment settings: Allow overwriting files and set our workspace
arcpy.env.workspace = workspace
arcpy.env.overwriteOutput = True

# LOGGING FUNCTION:
# This creates a "Human-Readable" history of the script's actions.
# Essential for troubleshooting and providing QC reports to supervisors.
def record_progress(message):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    formatted_msg = f"[{timestamp}] {message}"
    print(formatted_msg)
    with open(log_file, "a") as f:
        f.write(formatted_msg + "\n")

# -------------------------------------------------------------------------
# 2. THE MAIN PROCESSING ENGINE
# -------------------------------------------------------------------------

try:
    record_progress("--- Starting the Automated Batch Workflow ---")

    # Loop through each task defined in our dictionary above
    for layer_name, distance in tasks.items():
        
        # VALIDATION: Check if the layer actually exists.
        # This prevents the script from crashing if a file was accidentally deleted.
        if not arcpy.Exists(layer_name):
            record_progress(f"⚠️ SKIPPING {layer_name}: File not found in the database.")
            continue

        # GEOMETRY QC: Repair any "dirty" geometry (like self-intersections).
        # Counties often deal with legacy data; this ensures the shapes are "healthy."
        record_progress(f"Running Repair Geometry on {layer_name}...")
        arcpy.management.RepairGeometry(layer_name)

        # PROJECTION CHECK: Ensuring the data aligns with County Standards.
        # If the data is in Degrees (GPS), we must re-project it to Feet for accuracy.
        desc = arcpy.Describe(layer_name)
        if desc.spatialReference.factoryCode != county_standard.factoryCode:
            record_progress(f"SPATIAL ALIGNMENT: Projecting {layer_name} to NC State Plane (Feet).")
            # We use "memory/" to keep the hard drive clean of temporary files.
            working_layer = arcpy.management.Project(layer_name, "memory/temp_layer", county_standard)
        else:
            record_progress(f"SPATIAL VALIDATION: {layer_name} already matches standards.")
            working_layer = layer_name

        # ATTRIBUTE FILTERING: Example of a conditional analysis.
        # Here, we only want to buffer Elementary Schools for a specific safety study.
        if layer_name == "Schools":
            record_progress("Applying attribute filter for 'Elementary' schools...")
            working_layer = arcpy.management.SelectLayerByAttribute(working_layer, "NEW_SELECTION", "TYPE = 'Elementary'")

        # ANALYSIS & TOPOLOGY: Creating the actual Safety Buffers.
        record_progress(f"Creating a {distance} dissolved buffer for {layer_name}...")
        output_path = os.path.join(workspace, f"{layer_name}_Final_Buffer")
        
        
        
        # DISSOLVE_OPTION='ALL' is key: It merges overlapping shapes into a single polygon.
        # This is critical for clean, professional, "Map-Ready" results.
        arcpy.analysis.Buffer(
            in_features=working_layer,
            out_feature_class=output_path,
            buffer_distance_or_field=distance,
            dissolve_option="ALL" 
        )

        # FINAL VERIFICATION: Get the count of the results to confirm success.
        final_count = arcpy.management.GetCount(output_path)
        record_progress(f"✅ SUCCESS: {layer_name} processed. {final_count} zones created.")

    record_progress("--- Workflow Complete: Data is validated and ready for use ---")

except Exception as e:
    # CATCH-ALL ERROR HANDLING: If the script fails, it logs the exact Python error.
    # This prevents the window from just "disappearing" without an explanation.
    record_progress(f"❌ CRITICAL FAILURE: {str(e)}")

finally:
    # Cleanup: Delete the temporary memory layer to free up system RAM.
    if arcpy.Exists("memory/temp_layer"):
        arcpy.management.Delete("memory/temp_layer")