# gold_city_metrics.py
#
# Gold layer: City-level daily birding metrics for AI/BI dashboards
# Aggregates observations and checklists to provide city-level insights

from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ── Gold: City Daily Birding Metrics ──────────────────────────────────────────
@dp.materialized_view(
    name="city_daily_metrics_gold",
    comment="Daily rollup of birding activity and diversity by location. Optimized for AI/BI dashboards and trend analysis.",
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
    cluster_by=["location_key", "obs_date"],
)
def city_daily_metrics_gold():
    # Read silver tables
    index = spark.read.table("checklist_index_silver")
    detail = spark.read.table("checklist_detail_silver")
    observations = spark.read.table("observations_silver")
    
    # Join observations with index to get proper dates
    # (obs_date in observations_silver is often NULL due to parsing issues)
    # Drop obs_date and loc_id from observations to avoid ambiguity
    observations_with_dates = (
        observations
        .drop("obs_date", "loc_id")
        .join(
            index.select("sub_id", "obs_date", "loc_id"),
            "sub_id",
            "left"
        )
    )
    
    # Aggregate observations by location and date to get unique species count
    species_by_location = (
        observations_with_dates
        .filter("obs_date IS NOT NULL")
        .groupBy("loc_id", "obs_date")
        .agg(
            F.countDistinct("species_code").alias("unique_species_count"),
            F.count("obs_id").alias("total_observations")
        )
    )
    
    # Join index with detail - only select needed columns from detail to avoid conflicts
    index_with_detail = (
        index
        .join(
            detail.select("sub_id", "durationHrs", "allObsReported"),
            "sub_id",
            "left"
        )
    )
    
    # Join with species aggregation
    # Alias the join key columns to avoid conflicts after the join
    checklists = (
        index_with_detail
        .join(
            species_by_location.select(
                F.col("loc_id").alias("species_loc_id"),
                F.col("obs_date").alias("species_obs_date"),
                "unique_species_count",
                "total_observations"
            ), 
            (index_with_detail.loc_id == F.col("species_loc_id")) & 
            (index_with_detail.obs_date == F.col("species_obs_date")), 
            "left"
        )
        .drop("species_loc_id", "species_obs_date")
    )
    
    # Aggregate by location and date
    location_metrics = (
        checklists
        .groupBy("loc_id", "obs_date")
        .agg(
            # Location identifiers - use name if available, otherwise loc_id
            F.first(F.coalesce(F.col("locName"), F.col("loc_id"))).alias("location_name"),
            
            # Diversity metrics
            F.coalesce(F.max("unique_species_count"), F.lit(0)).alias("unique_species_count"),
            F.coalesce(F.sum("total_observations"), F.lit(0)).alias("total_observations"),
            
            # Activity metrics
            F.countDistinct("sub_id").alias("total_checklists"),
            F.countDistinct("userDisplayName").alias("unique_observers"),
            
            # Quality metrics
            F.avg("durationHrs").alias("avg_checklist_duration_hrs"),
            F.avg("numSpecies").alias("avg_species_per_checklist"),
            F.avg(F.when(F.col("allObsReported") == True, 1.0).otherwise(0.0)).alias("complete_checklist_rate"),
            
            # Geographic coordinates (average for the location)
            F.avg("lat").alias("avg_latitude"),
            F.avg("lng").alias("avg_longitude"),
            
            # Metadata
            F.min("_ingestion_ts").alias("first_ingestion_ts"),
            F.max("_ingestion_ts").alias("last_ingestion_ts"),
        )
        # Create a composite key for clustering
        .withColumn("location_key", F.col("loc_id"))
        # Add time dimension columns for easy filtering in AI/BI
        .withColumn("obs_year", F.year("obs_date"))
        .withColumn("obs_month", F.month("obs_date"))
        .withColumn("obs_day_of_week", F.dayofweek("obs_date"))
        .withColumn("obs_day_name", F.date_format("obs_date", "EEEE"))
        # Quality filters
        .filter("loc_id IS NOT NULL")
        .filter("obs_date IS NOT NULL")
        .select(
            "location_key",
            "location_name",
            "obs_date",
            "obs_year",
            "obs_month",
            "obs_day_of_week",
            "obs_day_name",
            "unique_species_count",
            "total_observations",
            "total_checklists",
            "unique_observers",
            "avg_checklist_duration_hrs",
            "avg_species_per_checklist",
            "complete_checklist_rate",
            "avg_latitude",
            "avg_longitude",
            "first_ingestion_ts",
            "last_ingestion_ts",
        )
    )
    
    return location_metrics
