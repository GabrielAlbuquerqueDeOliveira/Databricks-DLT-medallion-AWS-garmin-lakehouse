from pyspark.sql.functions import col, when, lower, coalesce, split, regexp_replace, expr
import re

# 1. Read Bronze (The "Vacuum Cleaner" phase - loads ALL CSVs)
df_bronze = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mergeSchema", "true") 
    .load("s3://datalake-sports-analytics-gabriel/bronze/*.csv")
    .withColumn("source_file", col("_metadata.file_path"))
)

# 2. Sanitize Column Names
def sanitize_columns(df):
    for column in df.columns:
        # Standardizing Garmin headers (removing special chars/spaces)
        clean_name = re.sub(r'[ ,;{}()\n\t=]', '_', column).replace('â', 'a').lower()
        df = df.withColumnRenamed(column, clean_name)
    return df

df_bronze_clean = sanitize_columns(df_bronze)

display(df_bronze_clean)

# 3. Unified Transformation logic
# We create a single 'sport_category' that works for both English (Kaggle) and Portuguese (Garmin)
df_silver_master = (
    df_bronze_clean
    .withColumn("sport_category", 
        when(lower(coalesce(col("sport_type"), col("tipo_de_atividade"))).contains("corrida") | 
             lower(coalesce(col("sport_type"), col("tipo_de_atividade"))).contains("run"), "Run")
        .when(lower(coalesce(col("sport_type"), col("tipo_de_atividade"))).contains("ciclismo") | 
             lower(coalesce(col("sport_type"), col("tipo_de_atividade"))).contains("ride"), "Ride")
        .when(lower(coalesce(col("sport_type"), col("tipo_de_atividade"))).contains("nata") | 
             lower(coalesce(col("sport_type"), col("tipo_de_atividade"))).contains("swim"), "Swim")
        .otherwise("Other")
    )
    # Mapping distance and time strings regardless of source
    .withColumn("dist_str", regexp_replace(coalesce(col("distance").cast("string"), col("distancia").cast("string")), ",", "."))
    .withColumn("time_str", regexp_replace(coalesce(col("moving_time").cast("string"), col("tempo").cast("string")), ",", "."))
)

# 4. Final Metrics Calculation (KM and Minutes)
time_splits = split(col("time_str"), ":")

df_silver_master = (
    df_silver_master
    .withColumn("standardized_distance_km", 
        when(col("source_file").contains("strava"), col("dist_str").cast("double") * 1.60934) # Convert Miles to KM
        .otherwise(col("dist_str").cast("double")) # Already in KM
    )
    .withColumn("standardized_time_minutes", 
        when(col("time_str").contains(":"), 
            (time_splits.getItem(0).cast("double") * 60) + 
            time_splits.getItem(1).cast("double") + 
            (time_splits.getItem(2).cast("double") / 60)
        )
        .otherwise(expr("try_cast(time_str as double)"))
    )
    # We keep only the main sports, but from BOTH sources
    .filter(col("sport_category").isin("Run", "Ride", "Swim"))
    .dropna(subset=["standardized_distance_km", "standardized_time_minutes"])
)

display(df_silver_master)

# --- THE VALIDATION (No filter, showing counts of both) ---
print("Total records in Silver Layer by source:")
df_silver_master.groupBy(
    when(col("source_file").contains("strava"), "Kaggle/Strava").otherwise("Garmin (Personal)")
).count().show()

display(df_silver_master)

# 5. Save to Silver Layer (This will contain the union of both datasets)
silver_path = "s3://datalake-sports-analytics-gabriel/silver/"
(df_silver_master.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(silver_path)
)

print(f"Success! Silver Layer saved at: {silver_path}")

# fafafafafa


# 6. Create the Garmin Gold (Personal Data Warehouse)
print("Creating Garmin Gold Warehouse...")
essential_columns = [
    "source_file",  
    "sport_category", 
    "standardized_distance_km", 
    "standardized_time_minutes",
    "título",
    "distancia",
    "tempo",
    "favorito",
    "fc_média",
    "fc_máxima",
    "te_aeróbico",
    "potência_média",
    "energia_máxima",
    "total_de_braçadas",
    "passos"
]
df_garmin_gold = (
    spark.read.format('delta').load(silver_path)        
    .filter(~col("source_file").contains("strava"))
    .select(*essential_columns)
)
display(df_garmin_gold)

# 4. Create the Global Kaggle "Warehouse"
print("Creating Kaggle Gold Warehouse...")
essential_columns = [
    "source_file",  
    "sport_category", 
    "standardized_distance_km", 
    "standardized_time_minutes",
    "name",
    "distance",
    "moving_time",
    "elapsed_time",
    "total_elevation_gain",
    "sport_type",
    "average_speed",
    "average_watts",
    "date",
    "time"

]
df_kaggle_gold = (
    spark.read.format('delta').load(silver_path)        
    .filter(col("source_file").contains("strava"))
    .select(*essential_columns)
)

display(df_kaggle_gold)

# S3 Destinations
garmin_path = "s3://datalake-sports-analytics-gabriel/gold/my_garmin_performance/"
kaggle_path = "s3://datalake-sports-analytics-gabriel/gold/global_strava/"

# Save Garmin Gold
print("Writing to Garmin Gold...")
(df_garmin_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sport_category") # Crucial for performance
    .save(garmin_path))

# Save Kaggle Gold
print("Writing to Kaggle Gold...")
(df_kaggle_gold.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("sport_category")
    .save(kaggle_path))

print("Gold Layer success! Check your S3 bucket now.")