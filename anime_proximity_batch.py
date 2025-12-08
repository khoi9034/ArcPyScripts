import arcpy
import os
import sys
import numpy as np
import csv
import traceback
import tempfile
from collections import defaultdict # Kept for completeness, though currently unused

# --- Scipy Check for Statistical Tests ---
try:
    from scipy import stats
    SCIPY_AVAILABLE = True
    print("✅ SciPy detected: statistical tests enabled.")
except ImportError:
    stats = None
    SCIPY_AVAILABLE = False
    print("⚠️ SciPy not found — Welch t-test will be skipped.")
    
# =====================================================
# CONFIGURATION CONSTANTS (Global for ALL Projects)
# =====================================================
print("\n[STEP 1] Setting up project configuration...")
arcpy.env.overwriteOutput = True

# --- DYNAMIC PROJECT DISCOVERY (Set your single base path here) ---
BASE_PROJECT_DIR = r"C:\ArcPyProjects\AutomatedAnimeStoreProximityByCity"

if not os.path.exists(BASE_PROJECT_DIR):
    print(f"❌ ERROR: Base project directory not found: {BASE_PROJECT_DIR}")
    sys.exit(1)  # Stop if the folder doesn't exist

# --- USER CONFIGURATION: Choose which cities to process ---
# Options:
# "all"      --> process all subfolders (all cities)
# ["Tokyo"]  --> process only Tokyo
# ["Tokyo", "Nagoya"] --> process multiple specific cities
PROCESS_CITIES = ["Tokyo"]

# --- Automatically find all city subdirectories ---
all_project_names = [d for d in os.listdir(BASE_PROJECT_DIR)
                    if os.path.isdir(os.path.join(BASE_PROJECT_DIR, d))]

if PROCESS_CITIES == "all":
    selected_projects = all_project_names
elif isinstance(PROCESS_CITIES, list):
    # Only include folders that exist
    selected_projects = [c for c in PROCESS_CITIES if c in all_project_names]
else:
    print("❌ Invalid value for PROCESS_CITIES. Must be 'all' or a list of city names.")
    sys.exit(1)

# Build full paths
PROJECT_FOLDERS_TO_PROCESS = [os.path.join(BASE_PROJECT_DIR, name) for name in selected_projects]

print(f"Projects queued: {selected_projects}")

# Define Target CRS: JGD2011 / UTM Zone 54N (WKID 6697)
TARGET_SR = arcpy.SpatialReference(6697)

# Output Layer Names (Standardized - Base names used within the active GDB)
GDB_LAYERS = {
    "boundary_proj": "Boundary_UTM_Cleaned",
    "anime_proj": "AnimeStores_UTM",
    "random_proj": "RandomPoints_UTM",
    "pop_centroids": "PopulationCentroids_UTM",
    "anime_near_table": "Anime_Near_Pop",
    "random_near_table": "Random_Near_Pop",
    "anime_count": "Municipalities_AnimeCount",
    "lisa_pop": "Municipalities_LISA",
    "hotspot_anime": "AnimeStores_GiHotSpot"
}

# =====================================================
# HELPER FUNCTIONS (UNCHANGED CORE LOGIC)
# =====================================================

def check_exists_layer(path, datatype="Feature Class"):
    """Checks if a feature class exists."""
    if not arcpy.Exists(path):
        raise FileNotFoundError(f"❌ Missing {datatype}: {path}")
    print(f"✔ Exists: {path}")

def near_distances_to_array(near_table):
    """Extracts distances from a Near Table's NEAR_DIST field and returns as a NumPy array."""
    dist = []
    with arcpy.da.SearchCursor(near_table, ["NEAR_DIST"]) as cur:
        for (d,) in cur:
            if d is not None:
                dist.append(d)
    return np.array(dist, dtype=float)

def near_table_to_lines(near_table, in_fc, near_fc, out_fc, target_sr):
    """Turns a Near Table into Polyline features."""
    print(f"Creating near lines: {os.path.basename(out_fc)}...")
    if arcpy.Exists(out_fc):
        arcpy.management.Delete(out_fc)
    
    # Dynamically determine the ID field name (OIDFieldName)
    in_id_field = arcpy.Describe(in_fc).OIDFieldName
    near_id_field = arcpy.Describe(near_fc).OIDFieldName
    
    arcpy.management.CreateFeatureclass(
        out_path=os.path.dirname(out_fc),
        out_name=os.path.basename(out_fc),
        geometry_type="POLYLINE",
        spatial_reference=target_sr
    )
    
    in_points = {row[0]: row[1] for row in arcpy.da.SearchCursor(in_fc, [in_id_field, "SHAPE@XY"])}
    near_points = {row[0]: row[1] for row in arcpy.da.SearchCursor(near_fc, [near_id_field, "SHAPE@XY"])}
    
    with arcpy.da.InsertCursor(out_fc, ["SHAPE@"]) as ins_cur:
        with arcpy.da.SearchCursor(near_table, ["IN_FID", "NEAR_FID"]) as near_cur:
            for in_fid, near_fid in near_cur:
                if in_fid in in_points and near_fid in near_points:
                    start = in_points[in_fid]
                    end = near_points[near_fid]
                    line = arcpy.Polyline(arcpy.Array([arcpy.Point(*start), arcpy.Point(*end)]))
                    ins_cur.insertRow([line])
    
    print(f"✅ Near lines created: {os.path.basename(out_fc)}")

def run_near_table(in_fc, near_fc, out_table): 
    """Generates a Near Table."""
    print(f"Generating near table: {out_table}...")
    if arcpy.Exists(out_table):
        arcpy.management.Delete(out_table)
    arcpy.analysis.GenerateNearTable(
        in_fc, near_fc, out_table, closest="1", method="GEODESIC"
    )
    print(f"✅ Near Table created: {out_table}")

def run_lisa(poly_fc, value_field, output_fc):
    """Runs Optimized Outlier Analysis (LISA)."""
    print("\n🔹 Running LISA analysis using OptimizedOutlierAnalysis...")
    if arcpy.Exists(output_fc):
        arcpy.management.Delete(output_fc)
    
    try:
        arcpy.stats.OptimizedOutlierAnalysis(
            Input_Features=poly_fc,
            Analysis_Field=value_field, # Using calculated 'pop_dens' field
            Output_Features=output_fc
        )
        print(f"✅ LISA analysis complete. Output saved at: {output_fc}")
    except Exception as e:
        print(f"❌ LISA analysis failed: {e}")
        raise

# =====================================================
# DYNAMIC FUNCTION BLOCK (Receives city-specific paths)
# =====================================================

# -----------------------------------------------------
# STEP 3 — Data Preparation and Rasterization (UTM)
# -----------------------------------------------------
def prepare_population_data(input_folder, output_folder, project_name, analysis_gdb, target_sr):
    """Projects boundary, joins population data, calculates density, and rasterizes for the current city."""
    
    print("\n[STEP 3] Preparing population data and rasterization...")
    
    # --- DYNAMIC INPUTS (Using Generic Names) ---
    BOUNDARY_FC_RAW = os.path.join(input_folder, "MunicipalBoundaries.shp") # <-- Generic Name
    POPULATION_CSV = os.path.join(input_folder, "PopulationData.csv")       # <-- Generic Name
    
    # --- OUTPUT PATHS ---
    POP_RASTER = os.path.join(output_folder, f"population_density_{project_name}_utm.tif")
    
    # --- INTERMEDIATE/OUTPUT PATHS (In the current city's GDB) ---
    boundary_projected = os.path.join(analysis_gdb, f"{project_name}_MunicipalBoundaries_UTM_Intermediate")
    boundary_joined_w_pop = os.path.join(analysis_gdb, f"{project_name}_MunicipalBoundaries_Joined_Intermediate")
    cleanedBoundariesJoined = os.path.join(analysis_gdb, GDB_LAYERS["boundary_proj"])

    # 1. Project raw boundary
    arcpy.management.Project(BOUNDARY_FC_RAW, boundary_projected, target_sr)
    print("✅ Boundary projected.")

    # 2. Join population CSV 
    layer_name = f"{project_name}_boundary_lyr"
    arcpy.management.MakeFeatureLayer(boundary_projected, layer_name)
    arcpy.management.AddJoin(layer_name, "ADM2_EN", POPULATION_CSV, "ADM2_EN")
    arcpy.management.CopyFeatures(layer_name, boundary_joined_w_pop)
    arcpy.management.RemoveJoin(layer_name)
    print(f"✅ Population CSV joined to boundary.")

    # 3. Add area_km2 field and calculate area
    arcpy.management.AddField(boundary_joined_w_pop, "area_km2", "DOUBLE")
    arcpy.management.CalculateField(boundary_joined_w_pop, "area_km2", "!shape.area@SQUAREKILOMETERS!", "PYTHON3")

    # 4. Calculate population density (using dynamic field discovery)
    arcpy.management.AddField(boundary_joined_w_pop, "pop_dens", "DOUBLE")
    
    pop_field_name = None
    all_fields = arcpy.ListFields(boundary_joined_w_pop) 
    
    for field in all_fields:
        if ("Pop" in field.name) and \
           (field.type in ('Float', 'Double', 'Integer', 'SmallInteger')) and \
           (not "ADM2_EN" in field.name) and \
           (not "area_km2" in field.name):
            
            pop_field_name = field.name
            break
            
    if pop_field_name is None:
        raise Exception(f"Could not find the NUMERIC population field after the CSV join in {project_name}. Check your CSV column names for 'Pop'.")
        
    print(f"ℹ️ Discovered NUMERIC Population Field Name: {pop_field_name}")

    expression = f"!{pop_field_name}! / !area_km2!"
    arcpy.management.CalculateField(
        in_table=boundary_joined_w_pop, 
        field="pop_dens", 
        expression=expression, 
        expression_type="PYTHON3"
    )
    print("🧮 Population density calculated.")

    # 5. Cap extreme population densities at the 99th percentile
    dens_values = [row[0] for row in arcpy.da.SearchCursor(boundary_joined_w_pop, ["pop_dens"]) if row[0] is not None]
    cap_value = np.percentile(dens_values, 99)

    with arcpy.da.UpdateCursor(boundary_joined_w_pop, ["pop_dens"]) as cursor:
        for row in cursor:
            if row[0] is not None and row[0] > cap_value:
                row[0] = cap_value
                cursor.updateRow(row)
    print("✅ Extreme pop_dens values capped.")

    # 6. Clean bad polygons and save final municipal layer
    arcpy.management.CopyFeatures(boundary_joined_w_pop, cleanedBoundariesJoined)
    bad_rows_deleted = 0
    with arcpy.da.UpdateCursor(cleanedBoundariesJoined, ["area_km2", "pop_dens"]) as cursor:
        for row in cursor:
            area, dens = row
            if area is None or dens is None or area <= 0 or dens <= 0:
                cursor.deleteRow()
                bad_rows_deleted += 1
    print(f"✅ Deleted {bad_rows_deleted} bad polygons. Cleaned data ready.")

    # 7. Rasterize population density
    arcpy.conversion.PolygonToRaster(
        in_features=cleanedBoundariesJoined,
        value_field="pop_dens",
        out_rasterdataset=POP_RASTER,
        cell_assignment="CELL_CENTER",
        cellsize=0.0016
    )
    arcpy.management.DefineProjection(POP_RASTER, target_sr)
    arcpy.management.CalculateStatistics(POP_RASTER)
    print(f"✅ Population raster created: {POP_RASTER}")
    
    return cleanedBoundariesJoined # Return the path to the final polygon layer

# -----------------------------------------------------
# STEP 4 — Point Data Setup (Anime and Random)
# -----------------------------------------------------
def setup_point_data(input_folder, analysis_gdb, boundary_fc_cleaned, target_sr):
    """Projects anime stores and generates a control set of random points for the current city."""
    print("\n[STEP 4] Setting up point data (Anime and Random)...")
    
    # --- DYNAMIC INPUT ---
    ANIME_FC_RAW = os.path.join(input_folder, "PointLocations_Raw.shp") # <-- Generic Name
    
    # --- OUTPUT PATHS (In the current city's GDB) ---
    anime_fc_proj = os.path.join(analysis_gdb, GDB_LAYERS["anime_proj"])
    random_fc_gdb = os.path.join(analysis_gdb, GDB_LAYERS["random_proj"])
    pop_centroids_fc = os.path.join(analysis_gdb, GDB_LAYERS["pop_centroids"])

    # 1. Project Anime Stores
    arcpy.management.Project(ANIME_FC_RAW, anime_fc_proj, target_sr)
    n_anime = int(arcpy.management.GetCount(anime_fc_proj).getOutput(0))
    if n_anime == 0:
        raise ValueError("Anime store point layer is empty after projection.")
    print(f"✅ Anime stores projected. Count: {n_anime}")

    # 2. Generate Random Points 
    with tempfile.TemporaryDirectory() as tmpdir:
        dissolved_fc = os.path.join(tmpdir, "dissolved_boundary.shp")
        arcpy.management.Dissolve(boundary_fc_cleaned, dissolved_fc)
        
        if arcpy.Exists(random_fc_gdb):
            arcpy.management.Delete(random_fc_gdb)

        # Generate the same number of random points as there are anime stores
        arcpy.management.CreateRandomPoints(
            out_path=analysis_gdb,
            out_name=GDB_LAYERS["random_proj"], # Output name relies on the active workspace
            constraining_feature_class=dissolved_fc,
            number_of_points_or_field=n_anime 
        )
    print(f"✅ Random points generated ({n_anime}).")

    # 3. Create Population Centroids
    arcpy.management.FeatureToPoint(boundary_fc_cleaned, pop_centroids_fc, "INSIDE")
    print("✅ Population centroids created.")
    
    return anime_fc_proj, random_fc_gdb, pop_centroids_fc

# -----------------------------------------------------
# STEP 5 — Proximity Analysis (Nearest Neighbor)
# -----------------------------------------------------
def run_proximity_analysis(anime_fc, random_fc, pop_fc, analysis_gdb, output_folder, project_name):
    """Calculates nearest neighbor distances and performs statistical comparison."""
    
    # Paths for output files outside the GDB (Must be full paths)
    RESULTS_CSV = os.path.join(output_folder, f"{project_name}_distribution_results.csv")
    NEAR_LINES_ANIME = os.path.join(output_folder, f"{project_name}_near_lines_anime.shp")
    NEAR_LINES_RANDOM = os.path.join(output_folder, f"{project_name}_near_lines_random.shp")

    # A. Anime Stores vs. Population Centroids
    anime_near_table = os.path.join(analysis_gdb, GDB_LAYERS["anime_near_table"])
    run_near_table(anime_fc, pop_fc, anime_near_table)
    near_table_to_lines(anime_near_table, anime_fc, pop_fc, NEAR_LINES_ANIME, TARGET_SR)

    # B. Random Points vs. Population Centroids
    random_near_table = os.path.join(analysis_gdb, GDB_LAYERS["random_near_table"])
    run_near_table(random_fc, pop_fc, random_near_table)
    near_table_to_lines(random_near_table, random_fc, pop_fc, NEAR_LINES_RANDOM, TARGET_SR)

    # C. Extract and Compare Distances
    anime_dist = near_distances_to_array(anime_near_table)
    random_dist = near_distances_to_array(random_near_table)

    # Calculate descriptive statistics
    mean_anime = np.mean(anime_dist)
    mean_random = np.mean(random_dist)
    med_anime = np.median(anime_dist)
    med_random = np.median(random_dist)
    sd_anime = np.std(anime_dist, ddof=1)
    sd_random = np.std(random_dist, ddof=1)

    t, p = None, None
    if SCIPY_AVAILABLE:
        t, p = stats.ttest_ind(
            anime_dist, random_dist,
            equal_var=False, nan_policy='omit'
        )

    # D. Output CSV
    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "anime_mean", "random_mean", "anime_median", "random_median", "anime_std", "random_std", "t_stat", "p_value", "city"])
        writer.writerow(["distance_to_population", mean_anime, mean_random, med_anime, med_random, sd_anime, sd_random, t, p, project_name])
    
    print(f"\n📊 Results saved: {RESULTS_CSV}")
    print(f"\n📊 Distance-to-Population Statistics for {project_name}:")
    print(f"Anime stores: Mean={mean_anime:.2f}m, Median={med_anime:.2f}m")
    if t is not None:
        print(f"Welch t-test: t_stat={t:.3f}, p_value={p:.4f}")

# -----------------------------------------------------
# STEP 6 — Spatial Clustering Analysis (Moran's I / Gi*)
# -----------------------------------------------------
def run_clustering_analysis(boundary_fc_cleaned, anime_fc, analysis_gdb):
    """Runs LISA on population density and Gi* on anime store counts for the current city."""

    # A. LISA on Population Density (Moran's I)
    lisa_output_pop = os.path.join(analysis_gdb, GDB_LAYERS["lisa_pop"])
    run_lisa(boundary_fc_cleaned, "pop_dens", lisa_output_pop)
    
    # B. Hot Spot Analysis (Gi*) for Anime Stores
    anime_count_fc = os.path.join(analysis_gdb, GDB_LAYERS["anime_count"])
    hotspot_output = os.path.join(analysis_gdb, GDB_LAYERS["hotspot_anime"])

    # i. Aggregate points to polygons (Count stores per municipal area)
    if arcpy.Exists(anime_count_fc):
        arcpy.management.Delete(anime_count_fc)

    arcpy.analysis.SummarizeWithin(
        in_polygons=boundary_fc_cleaned,
        in_sum_features=anime_fc,
        out_feature_class=anime_count_fc,
        keep_all_polygons=True 
    )

    # ii. Run Gi* Hot Spot Analysis on the aggregated counts
    arcpy.stats.HotSpots(
        Input_Feature_Class=anime_count_fc,
        Output_Feature_Class=hotspot_output,
        Input_Field="POINT_COUNT",
        Conceptualization_of_Spatial_Relationships="FIXED_DISTANCE_BAND",
        Distance_Band_or_Threshold_Distance="1500 Meters",
        Standardization="ROW"
    )

    print(f"🔥 Anime Store Hot Spot layer created: {hotspot_output}")

# =====================================================
# Core Batch Execution Function (The Loop Logic)
# =====================================================
def run_analysis_for_project(project_folder):
    """
    Runs the full analysis pipeline for a single defined project folder (city).
    This function contains the 'business logic' and is designed to run independently
    for each city in the batch loop. The try/except block ensures other cities
    will still process if one fails (Error Containment).
    """

    project_name = os.path.basename(project_folder) # Extract the city name (e.g., 'Tokyo') from the path.
    print("\n" + "="*80)
    print(f"🚀 STARTING ANALYSIS FOR: {project_name}")
    print("="*80)

    # --- Dynamic Path Configuration for the current project ---
    INPUT_FOLDER = os.path.join(project_folder, "inputs")  # Define the city-specific input path.
    OUTPUT_FOLDER = os.path.join(project_folder, "output") # Define the city-specific output path.

    # Validate structure
    if not os.path.exists(INPUT_FOLDER):
        print(f"❌ Skipping: Missing 'inputs' folder for {project_name}.") # Cannot proceed without inputs.
        return # Exit this function instance, allowing the main loop to continue to the next city.

    # Ensure output structure exists
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER) # Create the 'output' folder if it doesn't exist.

    # Create dynamic GDB name
    ANALYSIS_GDB = os.path.join(OUTPUT_FOLDER, f"{project_name}_analysis.gdb")
    if not arcpy.Exists(ANALYSIS_GDB):
        arcpy.management.CreateFileGDB(OUTPUT_FOLDER, f"{project_name}_analysis.gdb") # Create a unique GDB for this city.

    # **SCENARIO 1 KEY ACTION: Reset the workspace for the current project GDB**
    arcpy.env.workspace = ANALYSIS_GDB # CRITICAL: Sets the ArcPy environment so all tools use this GDB as the default output location.
    print(f"Workspace set to {ANALYSIS_GDB}")

    try:
        # STEP 3: Data Prep
        # Calls the function to project, join population data, calculate density, and rasterize.
        cleaned_boundary = prepare_population_data(INPUT_FOLDER, OUTPUT_FOLDER, project_name, ANALYSIS_GDB, TARGET_SR)

        # STEP 4: Point Setup
        # Calls the function to project anime stores, create random points, and get population centroids.
        anime_fc_proj, random_fc_gdb, pop_centroids_fc = setup_point_data(INPUT_FOLDER, ANALYSIS_GDB, cleaned_boundary, TARGET_SR)

        # STEP 5: Proximity Analysis
        # Calls the function to calculate near distances (Anime vs. Random) and run t-test.
        run_proximity_analysis(anime_fc_proj, random_fc_gdb, pop_centroids_fc, ANALYSIS_GDB, OUTPUT_FOLDER, project_name)

        # STEP 6: Clustering Analysis
        # Calls the function to run LISA (pop density) and Gi* Hot Spot (store counts).
        run_clustering_analysis(cleaned_boundary, anime_fc_proj, ANALYSIS_GDB)

        print(f"\n\n✅ ANALYSIS COMPLETE FOR: {project_name}")

    except Exception as e:
        # Error Containment: If any step (3, 4, 5, or 6) fails, this block catches it.
        print(f"\n🚨 ERROR IN {project_name}! Analysis failed.")
        print("Details:", e)
        traceback.print_exc() # Prints the full error stack for debugging.
        # Crucially, the function ends here, and the main loop continues to the next city.

# =====================================================
# Main execution loop (The Orchestrator)
# =====================================================
def main():
    """
    The orchestrator function. Its sole responsibility is to manage the 
    overall list of tasks and call the processing function for each one.
    """
    if not PROJECT_FOLDERS_TO_PROCESS:
        print("No project folders found to process. Check BASE_PROJECT_DIR path.")
        sys.exit(1) # Stop script if the initial project discovery failed.

    for project_folder in PROJECT_FOLDERS_TO_PROCESS: # Loop through every city folder discovered earlier.
        run_analysis_for_project(project_folder) # Delegates the work to the dedicated processing function.

if __name__ == "__main__":
    # The entry point: executes the main function when the script is run directly.
    main()