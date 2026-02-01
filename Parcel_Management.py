# ============================================================
# ONSLOW COUNTY–STYLE GIS AUTOMATION SCRIPT
# Author: Khoi Anh Nguyen
#
# PURPOSE:
# - Validate and clean parcel data
# - Enforce correct projection (NC State Plane)
# - Automate common county GIS tasks
# - Perform spatial analysis used by local government
# - Export a finished map product
#
# This script represents real-world county GIS workflows
# ============================================================


# -----------------------------
# IMPORT REQUIRED LIBRARIES
# -----------------------------

import arcpy              # ArcGIS Python site package
import os                 # For file and folder operations
import pandas as pd       # For CSV / tabular data cleanup (optional)


# -----------------------------
# ARCPY ENVIRONMENT SETTINGS
# -----------------------------

# Allow outputs to overwrite existing data
arcpy.env.overwriteOutput = True

# Set the working geodatabase (where all feature classes live)
arcpy.env.workspace = r"C:\GIS\OnslowCounty\OnslowGIS.gdb"

# Store workspace path for reuse
gdb = arcpy.env.workspace


# -----------------------------
# OUTPUT FOLDER SETUP
# -----------------------------

# Folder for exported maps and cleaned data
output_folder = r"C:\GIS\OnslowCounty\Outputs"

# Create the folder if it doesn't exist
if not os.path.exists(output_folder):
    os.makedirs(output_folder)


# -----------------------------
# INPUT DATA (FEATURE CLASSES)
# -----------------------------

# Core county GIS layers
parcels = "Parcels"
roads = "Roads"
county_boundary = "CountyBoundary"
floodzone = "FloodZone"
schools = "Schools"


# -----------------------------
# SPATIAL REFERENCE
# -----------------------------

# NC State Plane (Feet) — standard for North Carolina counties
nc_sp = arcpy.SpatialReference(32119)


# -----------------------------
# FUNCTION: ENSURE CORRECT PROJECTION
# -----------------------------

def ensure_projection(fc, spatial_ref):
    """
    Checks a feature class projection.
    If incorrect, creates a projected copy.
    Returns a feature class guaranteed to be in the correct CRS.
    """
    desc = arcpy.Describe(fc)

    # Compare spatial reference names
    if desc.spatialReference.name != spatial_ref.name:
        projected_fc = f"{fc}_proj"

        # Project to required coordinate system
        arcpy.Project_management(fc, projected_fc, spatial_ref)

        return projected_fc

    # If already correct, return original
    return fc


# -----------------------------
# PROJECTION VALIDATION
# -----------------------------

# Ensure parcels and roads are in NC State Plane
parcels = ensure_projection(parcels, nc_sp)
roads = ensure_projection(roads, nc_sp)


# -----------------------------
# GEOMETRY VALIDATION
# -----------------------------

# Fix invalid geometries (self-intersections, null shapes, etc.)
# This is VERY important for county parcel accuracy
arcpy.RepairGeometry_management(parcels)


# -----------------------------
# FIELD MANAGEMENT
# -----------------------------

# Get list of existing fields
existing_fields = [f.name for f in arcpy.ListFields(parcels)]

# Add acreage field if it doesn't exist
if "ACRES" not in existing_fields:
    arcpy.AddField_management(
        parcels,
        "ACRES",
        "DOUBLE"
    )

# Add normalized owner name field
if "OWNER_UPPER" not in existing_fields:
    arcpy.AddField_management(
        parcels,
        "OWNER_UPPER",
        "TEXT",
        field_length=100
    )


# -----------------------------
# FIELD CALCULATIONS
# -----------------------------

# Calculate parcel acreage using geometry
arcpy.CalculateField_management(
    parcels,
    "ACRES",
    "!shape.area@acres!",
    "PYTHON3"
)

# Normalize owner names to uppercase
# This improves consistency for joins and searches
with arcpy.da.UpdateCursor(parcels, ["OWNER", "OWNER_UPPER"]) as cursor:
    for row in cursor:
        if row[0]:                  # Check for null values
            row[1] = row[0].upper()
            cursor.updateRow(row)


# -----------------------------
# ATTRIBUTE DATA QUALITY CHECK
# -----------------------------

# Count parcels missing owner information
missing_owner_count = 0

with arcpy.da.SearchCursor(parcels, ["OWNER"]) as cursor:
    for row in cursor:
        if row[0] is None:
            missing_owner_count += 1

print(f"Parcels missing owner info: {missing_owner_count}")


# -----------------------------
# SPATIAL ANALYSIS TASKS
# -----------------------------

# 1️⃣ Buffer parcels (e.g., notification or impact zones)
parcel_buffer = "Parcels_Buffer_100ft"
arcpy.Buffer_analysis(
    parcels,
    parcel_buffer,
    "100 Feet"
)

# 2️⃣ Clip roads to county boundary
roads_clipped = "Roads_Clipped"
arcpy.Clip_analysis(
    roads,
    county_boundary,
    roads_clipped
)

# 3️⃣ Identify parcels in flood zones
parcels_flood = "Parcels_FloodRisk"
arcpy.Intersect_analysis(
    [parcels, floodzone],
    parcels_flood
)

# 4️⃣ Attach nearest school to each parcel
parcels_schools = "Parcels_Schools"
arcpy.SpatialJoin_analysis(
    parcels,
    schools,
    parcels_schools
)


# -----------------------------
# SQL SELECTION (COUNTY STYLE)
# -----------------------------

# Select large residential parcels
arcpy.MakeFeatureLayer_management(
    parcels,
    "Residential_Parcels",
    "ZONING = 'R1' AND ACRES > 0.25"
)


# -----------------------------
# BATCH PROCESSING
# -----------------------------

# Repair geometry on ALL feature classes in the GDB
# (This is common maintenance work in county GIS)
for fc in arcpy.ListFeatureClasses():
    arcpy.RepairGeometry_management(fc)


# -----------------------------
# CSV / TABULAR DATA CLEANUP
# -----------------------------

# Example: deed or tax data cleanup
csv_path = r"C:\GIS\OnslowCounty\Input\deeds.csv"

if os.path.exists(csv_path):
    df = pd.read_csv(csv_path)

    # Remove rows with missing values
    df.dropna(inplace=True)

    cleaned_csv = os.path.join(
        output_folder,
        "deeds_cleaned.csv"
    )

    df.to_csv(cleaned_csv, index=False)


# -----------------------------
# MAP EXPORT
# -----------------------------

# Access the currently open ArcGIS Pro project
aprx = arcpy.mp.ArcGISProject("CURRENT")

# Grab the first layout
layout = aprx.listLayouts()[0]

# Export a finished county map
pdf_path = os.path.join(
    output_folder,
    "Onslow_Parcel_Analysis.pdf"
)

layout.exportToPDF(pdf_path)


# -----------------------------
# SCRIPT COMPLETION
# -----------------------------

print("GIS processing complete. All county workflows executed successfully.")
