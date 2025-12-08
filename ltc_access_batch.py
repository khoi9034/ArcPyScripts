import arcpy
import os
import sys
import numpy as np
import csv
import traceback
import tempfile
from collections import defaultdict

# --- Scipy Check for Statistical Tests ---
# Kept for future use in the Comparative Analysis (e.g., comparing ward averages)
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
    print("✅ SciPy detected: statistical tests enabled.")
except ImportError:
    stats = None
    SCIPY_AVAILABLE = False
    print("⚠️ SciPy not found — Statistical tests will be skipped.")
    
# =====================================================
# CONFIGURATION CONSTANTS (Global for ALL Projects)
# =====================================================
print("\n[STEP 1] Setting up project configuration...")
arcpy.env.overwriteOutput = True

# --- DYNAMIC PROJECT DISCOVERY (Set your single base path here) ---
BASE_PROJECT_DIR = r"C:\ArcPyProjects\AutomatedLongTermCareAccessByCity" # Changed path name

if not os.path.exists(BASE_PROJECT_DIR):
    print(f"❌ ERROR: Base project directory not found: {BASE_PROJECT_DIR}")
    sys.exit(1)

# --- USER CONFIGURATION: Choose which cities to process ---
PROCESS_CITIES = ["Tokyo"] # Set to "Tokyo" for now

all_project_names = [d for d in os.listdir(BASE_PROJECT_DIR)
                     if os.path.isdir(os.path.join(BASE_PROJECT_DIR, d))]

if PROCESS_CITIES == "all":
    selected_projects = all_project_names
elif isinstance(PROCESS_CITIES, list):
    selected_projects = [c for c in PROCESS_CITIES if c in all_project_names]
else:
    print("❌ Invalid value for PROCESS_CITIES.")
    sys.exit(1)

PROJECT_FOLDERS_TO_PROCESS = [os.path.join(BASE_PROJECT_DIR, name) for name in selected_projects]

print(f"Projects queued: {selected_projects}")

# Define Target CRS: JGD2011 / UTM Zone 54N (WKID 6697)
TARGET_SR = arcpy.SpatialReference(6697)

# Output Layer Names (Standardized - Base names used within the active GDB)
GDB_LAYERS = {
    "boundary_proj": "WardBoundaries_UTM_Cleaned",
    "facility_proj": "LTCCareFacilities_UTM", # Changed name
    "pop_density": "ElderlyPopDensity_MapData",
    "analysis_data": "WardData_Final"
}

# =====================================================
# HELPER FUNCTIONS (Adjusted to include target_sr where needed)
# =====================================================

def check_exists_layer(path, datatype="Feature Class"):
    """Checks if a feature class exists."""
    if not arcpy.Exists(path):
        raise FileNotFoundError(f"❌ Missing {datatype}: {path}")
    print(f"✔ Exists: {path}")

# (near_distances_to_array, near_table_to_lines, run_near_table, run_lisa removed as they are not needed for the Descriptive Analysis section)

# =====================================================
# DYNAMIC FUNCTION BLOCK (Receives city-specific paths)
# =====================================================

# -----------------------------------------------------
# STEP 3 — Data Preparation: Elderly Density (FIGURE 7)
# -----------------------------------------------------
def prepare_population_data(input_folder, output_folder, project_name, analysis_gdb, target_sr):
    """Projects boundary, joins ELDERLY population data, and calculates % Aged 65+."""
    
    print("\n[STEP 3] Preparing elderly population density data...")
    
    # --- DYNAMIC INPUTS (Using Generic Names) ---
    BOUNDARY_FC_RAW = os.path.join(input_folder, "MunicipalBoundaries.shp") # Ward Polygons
    POPULATION_CSV = os.path.join(input_folder, "ElderlyPopulationData.csv") # Must contain ADM2_EN, POP_TOTAL, and POP_65PLUS
    
    # --- INTERMEDIATE/OUTPUT PATHS (In the current city's GDB) ---
    boundary_projected = os.path.join(analysis_gdb, f"{project_name}_WardBoundaries_UTM_Intermediate")
    boundary_joined_w_pop = os.path.join(analysis_gdb, f"{project_name}_WardBoundaries_Joined_Intermediate")
    final_ward_data = os.path.join(analysis_gdb, GDB_LAYERS["analysis_data"])

    # 1. Project raw boundary
    arcpy.management.Project(BOUNDARY_FC_RAW, boundary_projected, target_sr)
    print("✅ Ward Boundary projected.")

    # 2. Join Elderly Population CSV 
    # NOTE: CSV must have fields for Total Population (POP_TOTAL) and Population 65+ (POP_65PLUS).
    layer_name = f"{project_name}_boundary_lyr"
    arcpy.management.MakeFeatureLayer(boundary_projected, layer_name)
    arcpy.management.AddJoin(layer_name, "ADM2_EN", POPULATION_CSV, "ADM2_EN")
    arcpy.management.CopyFeatures(layer_name, boundary_joined_w_pop)
    arcpy.management.RemoveJoin(layer_name)
    print(f"✅ Elderly Population CSV joined to boundary.")

    # 3. Add density field and calculate percentage of elderly population
    arcpy.management.AddField(boundary_joined_w_pop, "ELD_PCT", "DOUBLE")
    
    # --- Dynamic Field Discovery for Population Counts ---
    # Find the fields for the total population and the 65+ population after the join.
    pop_total_field = None
    pop_65plus_field = None
    all_fields = arcpy.ListFields(boundary_joined_w_pop) 
    
    for field in all_fields:
        # We assume the fields contain 'POP_TOTAL' and 'POP_65PLUS' and are numeric.
        if (field.type in ('Float', 'Double', 'Integer', 'SmallInteger')) and (not "ADM2_EN" in field.name):
            if "POP_TOTAL" in field.name:
                pop_total_field = field.name
            elif "POP_65PLUS" in field.name:
                pop_65plus_field = field.name
            
    if pop_total_field is None or pop_65plus_field is None:
        found_names = [f.name for f in all_fields]
        raise Exception(f"Could not find numeric 'POP_TOTAL' and 'POP_65PLUS' fields in {project_name}. Fields found: {found_names}")
        
    print(f"ℹ️ Discovered Total Pop Field: {pop_total_field}, 65+ Pop Field: {pop_65plus_field}")

    # Calculate Percentage Elderly: (POP_65PLUS / POP_TOTAL) * 100
    # Add a small buffer (+1) to the denominator to prevent division by zero in case of empty polygons, although unlikely for wards.
    expression = f"(!{pop_65plus_field}! / (!{pop_total_field}! + 1)) * 100"
    
    arcpy.management.CalculateField(
        in_table=boundary_joined_w_pop, 
        field="ELD_PCT", 
        expression=expression, 
        expression_type="PYTHON3"
    )
    print("🧮 Elderly population percentage (ELD_PCT) calculated.")

    # 4. Clean bad/missing data and save final municipal layer
    arcpy.management.CopyFeatures(boundary_joined_w_pop, final_ward_data)
    
    # Optional: Clean any rows where ELD_PCT is null or 0 (shouldn't happen with wards but good practice)
    bad_rows_deleted = 0
    with arcpy.da.UpdateCursor(final_ward_data, ["ELD_PCT"]) as cursor:
        for row in cursor:
            if row[0] is None or row[0] <= 0:
                cursor.deleteRow()
                bad_rows_deleted += 1
    print(f"✅ Deleted {bad_rows_deleted} bad polygons. Cleaned data ready.")
    
    return final_ward_data # Return the path to the final polygon layer for FIGURE 7 & 8

# -----------------------------------------------------
# STEP 4 — Facility Data Setup (FIGURE 6)
# -----------------------------------------------------
def setup_facility_data(input_folder, analysis_gdb, target_sr):
    """Projects Long-Term Care Facility point data."""
    print("\n[STEP 4] Setting up Long-Term Care Facility data...")
    
    # --- DYNAMIC INPUT ---
    FACILITY_FC_RAW = os.path.join(input_folder, "LTCCareFacilities_Raw.shp") # Raw Point Locations
    
    # --- OUTPUT PATHS (In the current city's GDB) ---
    facility_fc_proj = os.path.join(analysis_gdb, GDB_LAYERS["facility_proj"])

    # 1. Project Facilities
    arcpy.management.Project(FACILITY_FC_RAW, facility_fc_proj, target_sr)
    n_facilities = int(arcpy.management.GetCount(facility_fc_proj).getOutput(0))
    if n_facilities == 0:
        raise ValueError("Long-Term Care Facility layer is empty after projection.")
    print(f"✅ Facilities projected. Count: {n_facilities}")

    # Note: No random points or centroids are needed for the descriptive maps, 
    # but the facility layer (facility_fc_proj) is required for FIGURE 6 and FIGURE 8.
    
    return facility_fc_proj

# -----------------------------------------------------
# STEP 5 — PLACEHOLDER: Advanced Analysis (Future)
# -----------------------------------------------------
def run_placeholder_analysis(ward_data, facility_data, output_folder, project_name):
    """
    Placeholder for future analysis: 
    * Advanced Spatial Statistics (e.g., Location Quotient, Hot Spot Analysis on Facility Count)
    * Network/Service Area Analysis (for Accessibility 'A')
    """
    print("\n[STEP 5] Placeholder: Advanced accessibility analysis (Skipped for Abstract/Descriptive sections).")
    
    # This step would typically involve:
    # 1. SummarizeWithin to count facilities per ward.
    # 2. Calculating the Location Quotient (LQ) of facilities vs. elderly population.
    # 3. Running a spatial regression (e.g., GWR) or Hot Spot analysis on the LQ score.
    
    pass


# =====================================================
# Core Batch Execution Function (The Loop Logic)
# =====================================================
def run_analysis_for_project(project_folder):
    """Runs the full analysis pipeline for a single project folder (city)."""

    project_name = os.path.basename(project_folder)
    print("\n" + "="*80)
    print(f"🚀 STARTING ANALYSIS FOR: {project_name}")
    print("="*80)

    # --- Dynamic Path Configuration ---
    INPUT_FOLDER = os.path.join(project_folder, "inputs")
    OUTPUT_FOLDER = os.path.join(project_folder, "output")
    
    if not os.path.exists(INPUT_FOLDER):
        print(f"❌ Skipping: Missing 'inputs' folder for {project_name}.")
        return

    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)

    # Create dynamic GDB name
    ANALYSIS_GDB = os.path.join(OUTPUT_FOLDER, f"{project_name}_analysis.gdb")
    if not arcpy.Exists(ANALYSIS_GDB):
        arcpy.management.CreateFileGDB(OUTPUT_FOLDER, f"{project_name}_analysis.gdb")

    arcpy.env.workspace = ANALYSIS_GDB
    print(f"Workspace set to {ANALYSIS_GDB}")

    try:
        # STEP 3: Data Prep (Calculates ELD_PCT for FIGURE 7)
        ward_data = prepare_population_data(INPUT_FOLDER, OUTPUT_FOLDER, project_name, ANALYSIS_GDB, TARGET_SR)

        # STEP 4: Facility Setup (Projects points for FIGURE 6 and FIGURE 8)
        facility_data = setup_facility_data(INPUT_FOLDER, ANALYSIS_GDB, TARGET_SR)

        # STEP 5: Placeholder for Advanced Analysis
        run_placeholder_analysis(ward_data, facility_data, OUTPUT_FOLDER, project_name)

        print(f"\n\n✅ DATA PREP COMPLETE FOR MAPPING: {project_name}")
        print("Data for map layers (Figures 6, 7, 8) available in:")
        print(f"  - Elderly Density Polygons (Figure 7): {ward_data} (ELD_PCT field)")
        print(f"  - Facility Points (Figure 6 & 8 Overlay): {facility_data}")

    except Exception as e:
        print(f"\n🚨 ERROR IN {project_name}! Analysis failed.")
        print("Details:", e)
        traceback.print_exc()
        sys.exit(1) # Stop script if core data prep fails

# =====================================================
# Main execution loop (The Orchestrator)
# =====================================================
def main():
    """Manages the overall list of tasks and calls the processing function for each city."""
    if not PROJECT_FOLDERS_TO_PROCESS:
        print("No project folders found to process.")
        sys.exit(1)

    for project_folder in PROJECT_FOLDERS_TO_PROCESS:
        run_analysis_for_project(project_folder)

if __name__ == "__main__":
    main()