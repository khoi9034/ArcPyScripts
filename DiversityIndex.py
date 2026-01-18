import arcpy
import os

# 1. ENVIRONMENT SETUP
# Overwrite allows the script to run even if the output file already exists
arcpy.env.overwriteOutput = True

# Define file paths
input_tracts = r"C:\ArcPyProjects\DiversityIndex\inputs\dm10\NCTracts2010.shp"
output_folder = r"C:\ArcPyProjects\DiversityIndex\outputs"
gdb_name = "DiversityIndex.gdb"

# Professional Pathing: Joins folder and name correctly for any computer
output_gdb = os.path.join(output_folder, gdb_name)

# 2. GEODATABASE CREATION
# Only create the GDB if it doesn't already exist to save time/resources
if not arcpy.Exists(output_gdb):
    arcpy.management.CreateFileGDB(output_folder, gdb_name)
    print(f"Created new GDB: {output_gdb}")

# 3. SCHEMA DISCOVERY (Your efficient logic)
# ListFields scans the metadata so we know what columns we have to work with
fields = arcpy.ListFields(input_tracts)
fieldNames = [field.name for field in fields]
print(f"Detected fields: {fieldNames}")

# Define the "Room Number" (The specific feature class inside the GDB)
county_name = "Orange"
out_path = os.path.join(output_gdb, f"{county_name}_Diversity_2010")

# 4. FIELD MAPPING (The Decoder Ring)
# Maps our math logic to the actual messy census column names
field_map = {
    "pop": "POP2010",
    "white": "WHITE",
    "black": "BLACK",
    "ameri": "AMERI_ES",
    "asian": "ASIAN",
    "HAWNPI":"HAWN_PI",
    "other": "OTHER",
    "hisp": "HISPANIC"
}

# 5. DATA VALIDATION
# Use .items() to check both our nickname (key) and the real data name (val)
for key,val in field_map.items():
    if val not in fieldNames:
        raise ValueError(f"Error: the key {key} for {val}")
    else:
        print(f"Key and value pairs founded: {key} -> {val}")

# 6. SPATIAL SELECTION (Bonus Step)
# We must convert the file to a 'Layer' to perform an attribute selection
temp_layer = "nc_tracts_layer"
arcpy.management.MakeFeatureLayer(input_tracts, temp_layer)

# Select Orange County using its unique Federal FIPS code (135)
print(f"Filtering data for {county_name} County...")
arcpy.management.SelectLayerByAttribute(temp_layer, "NEW_SELECTION", "CNTY_FIPS = '135'")

# Copy only the selected tracts into our Geodatabase
arcpy.management.CopyFeatures(temp_layer, out_path)

# 7. ADDING CALCULATED FIELDS
# We add empty 'buckets' to the table to hold our results
new_fields = ["div_index", "per_Nhisp"]
for nf in new_fields:
    # Only add the field if it doesn't exist yet in the NEW output file
    if nf not in [f.name for f in arcpy.ListFields(out_path)]:
        arcpy.management.AddField(out_path, nf, "FLOAT")

# 8. THE MATH ENGINE (UpdateCursor)
# We define exactly which columns to look at, in a specific index order
#INDEXED 0-9
cursor_fields = [ 
    field_map["pop"],    # Index 0
    field_map["white"],  # Index 1
    field_map["black"],  # Index 2
    field_map["ameri"],  # Index 3
    field_map["asian"],  # Index 4
    field_map["HAWNPI"], # Index 5
    field_map["other"],  # Index 6
    field_map["hisp"],   # Index 7
    "div_index",         # Index 8
    "per_Nhisp"          # Index 9
]

# Use a 'with' block to safely open/close the table connection
with arcpy.da.UpdateCursor(out_path, cursor_fields) as cursor:
    for row in cursor:
        # row[0] is POP2000. Use 'or 0' to avoid crashing on empty/Null cells
        pop = row[0] if row[0] and row[0] > 0 else 0
        
        if pop > 0:
            # Race math: Sum of (Population Group / Total Population)^2
            # range(1, 7) looks at indices 1, 2, 3, 4, 5, 6 (White through Other)
            race_sq_sum = sum([((row[i] or 0) / pop)**2 for i in range(1, 7)])
            
            # Ethnicity math: (Hispanic %)^2 + (Non-Hispanic %)^2
            per_hisp = (row[7] or 0) / pop
            per_Nhisp = 1 - per_hisp
            eth_sq_sum = (per_hisp**2) + (per_Nhisp**2)
            
            # Simpson's Diversity Formula: 1 - (Race Diversity * Ethnic Diversity)
            row[9] = per_Nhisp # Store per_Nhisp at index 9
            row[8] = 1 - (race_sq_sum * eth_sq_sum) # Store DI at index 8
        else:
            # If no population, diversity is zero
            row[8], row[9] = 0, 0
            
        # Push the memory calculation into the actual physical table
        cursor.updateRow(row)

# 9. DATA CLEANUP
# Final pass to handle 'outliers' where diversity is mathematically 1.0 (usually errors)
with arcpy.da.UpdateCursor(out_path, ["div_index"]) as cursor:
    for row in cursor:
        if row[0] >= 0.999:
            row[0] = 0
            cursor.updateRow(row)

print(f"SUCCESS: Result saved to {out_path}")