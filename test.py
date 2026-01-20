import arcpy
import os 
arcpy.env.overwriteOutput = True

inputTract= r"C:\ArcPyProjects\DiversityIndex\inputs\dm10\NCTracts2010.shp"
output_folder = r"C:\ArcPyProjects\DiversityIndex\outputs"
gdb_name = "DiversityIndex.gdb"
outgdb = os.path.join(output_folder,gdb_name)
chosenCounty = "Orange"
outPath= os.path.join(outgdb, f"{chosenCounty}_DI_2010")

fieldsNameTract = [f.name for f in arcpy.ListFields(inputTract)]
print(f"Fields in the input data: {fieldsNameTract}") 
if not arcpy.Exists(outgdb):
    arcpy.management.CreateFileGDB(output_folder, gdb_name)
else:
    print(f"GDB already exist:{outgdb}")

if not arcpy.Exists(outPath):
    FieldsToKeep = ['POP2010','WHITE', 'BLACK', 'AMERI_ES', 'ASIAN', 'HAWN_PI', 'HISPANIC', 'OTHER','CNTY_FIPS' ]
    fms = arcpy.FieldMappings() # creating container for all the collumns 
    fms.addTable(inputTract)

    for field in fms.fields: #this is the first safty net creates a static list of the table so doesent shift wheen we delete fields
        if field.name not in FieldsToKeep and not field.required:
            fms.removeFieldMap(fms.findFieldMapIndex(field.name))


    arcpy.conversion.ExportFeatures(

        in_features=inputTract,
        out_features=outPath,
        where_clause="CNTY_FIPS = '135'",
        field_mapping=fms
    )
    print(f"Feature class created:{outPath}")
else:
    print(f"Feature class already exist:{outPath}")

listFields_outPathCleaned = [f.name for f in arcpy.ListFields(outPath)]
print(f"Fields in outPath before adding new fields: {listFields_outPathCleaned}")

newfields = ["div_index","per_Nhisp"]  

for nf in newfields:
    if nf not in listFields_outPathCleaned:
        arcpy.management.AddField(outPath,nf, "DOUBLE")
    else:
        print("All fields already exist")

#NOW that all fields are cleaned and ready I will now create the blueprint for the cursor 


#but first i have to make a fieldmap to rename the fields to easier names to use later

field_map ={
    "pop":"POP2010",
    "white":"WHITE",
    "black":"BLACK",
    "ameri":"AMERI_ES",
    "asian":"ASIAN",
    "hawnpi":"HAWN_PI",
    "hisp":"HISPANIC",
    "other":"OTHER",
}

#making sure the values exist in the cleaned field list. have to loop thorugh keya nd val becuase fieldmap  items returns a pair.
for key, val in field_map.items():
    if val not in listFields_outPathCleaned:
        raise ValueError(f"Error: the key {key} for {val} not found in the fields")
    else:
        print(f"Key and value pairs found: {key} -> {val}")



cursorfields = [
    field_map["pop"],        #0
    field_map["white"],      #1
    field_map["black"],      #2
    field_map["ameri"],      #3
    field_map["asian"],      #4
    field_map["hawnpi"],     #5
    field_map["hisp"],       #6
    field_map["other"],      #7
    "div_index",             #8
    "per_Nhisp"              #9

]


with arcpy.da.UpdateCursor(outPath, cursorfields) as cursor:
    for row in cursor:
        pop = row[0] if row[0] and row[0]>0 else 0
        if pop > 0:
            allracesquaresum = sum([((row[i])/100)**2 for i in range(1,)])
            perHisp = (row[6] or 0)/pop
            perNhisp = 1- perHisp
            hisp_nhips_squared = perHisp**2 + perNhisp**2
            row[8] = 1 - (allracesquaresum * hisp_nhips_squared)
            row[9] = perNhisp
        else:
            row[8]= 0
            row[9]= 0
        cursor.updateRow(row)

#This is another way to set up the conditions of the loop for the cursor.
# with arcpy.da.UpdateCursor(outPath, cursorfields) as cursor:
#     for row in cursor:
#         if row[0] and row[0]>0:
#             pop = row[0]
#             sum_race_squared = sum([((row[i])/pop)**2 for i in range(1,7)])
#             perHisp = (row[6] or 0)/pop
#             per_Nhisp = 1 - perHisp
#             his_nhisp_squared = perHisp**2 + per_Nhisp**2
#             row[8] = 1 - (sum_race_squared * his_nhisp_squared)
#             row[9] = per_Nhisp



            
#         else:# if pop is 0 or null ill preset these values for it
#             row[8]= 0
#             row[9]= 0
#         cursor.updateRow(row)


with arcpy.da.UpdateCursor(outPath, ["div_index"]) as cursor:
    for row in cursor:
        if row[0] >= .9999:
            row[0]= 0
            cursor.updateRow(row)

print(f"Diversity Index calculation completed. Results saved in: {outPath}")





    
   


           
   
















   




