import dlt
from pyspark.sql.functions import col, current_timestamp, sum, max, when

# --- 1. BRONZE LAYER ---
@dlt.table(
    name="garmin_bronze",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
    }
)
def garmin_bronze():
    # Ingesting raw CSVs from S3 using Auto Loader
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load("s3://datalake-sports-analytics-gabriel/bronze/")
    )

# --- 2. SILVER LAYER ---
@dlt.table(name="garmin_silver")
@dlt.expect_or_drop("valid_distance", "distance_km > 0 AND distance_km < 100")
def garmin_silver():
    return (
        dlt.read_stream("garmin_bronze")
        .select(
            col("Data").alias("event_date"),
            # Primeiro, definimos o tipo em inglês
            when(col("`Tipo de atividade`").contains("Corrida"), "Run")
            .when(col("`Tipo de atividade`").contains("Natação"), "Swim")
            .when(col("`Tipo de atividade`").contains("Ciclismo"), "Ride")
            .otherwise("Other")
            .alias("activity_type"),
            # Agora a conversão: Se for Swim (que acabamos de definir), divide por 1000
            # Usamos o nome original da coluna de distância aqui para garantir o cast
            when(col("`Tipo de atividade`").contains("Natação"), 
                 col("`Distância`").cast("double") / 1000)
            .otherwise(col("`Distância`").cast("double"))
            .alias("distance_km"),
            current_timestamp().alias("ingestion_timestamp")
        )
        .dropDuplicates(["event_date", "activity_type", "distance_km"])
    )

# --- 3. GOLD LAYER ---
@dlt.table(name="performance_summary_gold")
def performance_summary_gold():
    # Aggregating clean data for the dashboard
    return (
        dlt.read("garmin_silver")
        .groupBy("event_date", "activity_type")
        .agg(
            sum("distance_km").alias("total_distance_km"),
            max("ingestion_timestamp").alias("last_update")
        )
    )