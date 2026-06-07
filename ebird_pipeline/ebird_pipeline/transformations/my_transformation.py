# pipeline_ebird_bronze.py  (DLT pipeline notebook)
#
# Attach this as a Delta Live Tables pipeline in Databricks, not a regular job.
# Auto Loader watches the Volume paths and processes new files as they arrive.
#
# Pipeline settings (set in the DLT UI or pipeline JSON):
#   catalog  : birds
#   target   : ebird_bronze
#   channel  : current

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, TimestampType, DoubleType

VOLUME_ROOT = "/Volumes/birds/ebird_landing/raw"

# ── Bronze: checklist_index ───────────────────────────────────────────────────
@dlt.table(
    name="checklist_index_bronze",
    comment="Raw eBird checklist index — one row per checklist stub, payload intact",
    table_properties={
        "quality":                        "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["_run_date"],
)
@dlt.expect("envelope_present", "raw_json IS NOT NULL")
def checklist_index_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",          "json")
             .option("cloudFiles.inferColumnTypes", "false")
             .option("cloudFiles.schemaLocation",
                     f"{VOLUME_ROOT}/checklist_index/_schema_hint")
             .load(f"{VOLUME_ROOT}/checklist_index/")
             .select(
                 F.col("_run_id"),
                 F.col("_ingestion_ts").cast(TimestampType()),
                 F.col("_run_date"),
                 F.col("_source"),
                 F.col("_page_num").cast(IntegerType()),
                 F.col("payload").alias("raw_json"),  # payload is already a JSON string
                 F.col("_metadata.file_path").alias("_source_file"),
                 F.col("_metadata.file_modification_time").alias("_file_modified_ts"),
             )
    )


# ── Bronze: checklist_detail ──────────────────────────────────────────────────
@dlt.table(
    name="checklist_detail_bronze",
    comment="Raw eBird checklist detail — one row per checklist, payload intact. Error rows included.",
    table_properties={
        "quality":                        "bronze",
        "pipelines.autoOptimize.managed": "true",
    },
    partition_cols=["_run_date"],
)
@dlt.expect("sub_id_present", "get_json_object(raw_json, '$.subId') IS NOT NULL OR _fetch_error IS NOT NULL")
def checklist_detail_bronze():
    return (
        spark.readStream
             .format("cloudFiles")
             .option("cloudFiles.format",          "json")
             .option("cloudFiles.inferColumnTypes", "false")
             .option("cloudFiles.schemaLocation",
                     f"{VOLUME_ROOT}/checklist_detail/_schema_hint")
             .load(f"{VOLUME_ROOT}/checklist_detail/")
             .select(
                 F.col("_run_id"),
                 F.col("_ingestion_ts").cast(TimestampType()),
                 F.col("_run_date"),
                 F.col("_source"),
                 F.col("_http_status").cast(IntegerType()),
                 F.col("_fetch_error"),
                 F.col("payload").alias("raw_json"),  # payload is already a JSON string
                 F.col("_metadata.file_path").alias("_source_file"),
                 F.col("_metadata.file_modification_time").alias("_file_modified_ts"),
             )
    )


# ── Silver: checklist_index ───────────────────────────────────────────────────
@dlt.table(
    name="checklist_index_silver",
    comment="Parsed and typed checklist index. One row per checklist stub.",
    table_properties={"quality": "silver"},
    partition_cols=["obs_date"],
)
@dlt.expect_or_drop("valid_sub_id",       "sub_id IS NOT NULL")
@dlt.expect_or_drop("valid_obs_dt",       "obs_dt IS NOT NULL")
@dlt.expect(        "valid_species_count", "numSpecies > 0")
def checklist_index_silver():
    return (
        dlt.read_stream("checklist_index_bronze")
           # Extract key fields using get_json_object
           .withColumn("sub_id", F.get_json_object("raw_json", "$.subId"))
           .withColumn("numSpecies", F.get_json_object("raw_json", "$.numSpecies").cast(IntegerType()))
           .withColumn("isoObsDate", F.get_json_object("raw_json", "$.isoObsDate"))
           .withColumn("obs_dt", F.to_timestamp("isoObsDate", "yyyy-MM-dd HH:mm"))
           .withColumn("obs_date", F.to_date("isoObsDate", "yyyy-MM-dd HH:mm"))
           .withColumn("loc_id", F.coalesce(
               F.get_json_object("raw_json", "$.loc.locId"),
               F.get_json_object("raw_json", "$.loc.locID")
           ))
           .withColumn("lat", F.get_json_object("raw_json", "$.loc.lat").cast(DoubleType()))
           .withColumn("lng", F.get_json_object("raw_json", "$.loc.lng").cast(DoubleType()))
           .withColumn("locName", F.get_json_object("raw_json", "$.locName"))
           .withColumn("userDisplayName", F.get_json_object("raw_json", "$.userDisplayName"))
           .select(
               F.col("sub_id"),
               F.col("obs_dt"),
               F.col("obs_date"),
               F.col("numSpecies"),
               F.col("loc_id"),
               F.col("lat"),
               F.col("lng"),
               F.col("locName"),
               F.col("userDisplayName"),
               F.col("_run_id"),
               F.col("_ingestion_ts"),
               F.col("_run_date"),
               F.col("_page_num"),
               F.col("_source_file"),
           )
    )


# ── Silver: checklist_detail ──────────────────────────────────────────────────
@dlt.table(
    name="checklist_detail_silver",
    comment="Parsed checklist detail. Error rows excluded — see bronze for full audit.",
    table_properties={"quality": "silver"},
    partition_cols=["obs_date"],
)
@dlt.expect_or_drop("no_fetch_error", "_fetch_error IS NULL")
@dlt.expect_or_drop("valid_sub_id",   "sub_id IS NOT NULL")
@dlt.expect(        "has_species",    "numSpecies > 0") 
def checklist_detail_silver():
    return (
        dlt.read_stream("checklist_detail_bronze")
           # Extract key fields using get_json_object
           .withColumn("sub_id", F.get_json_object("raw_json", "$.subId"))
           .withColumn("numSpecies", F.get_json_object("raw_json", "$.numSpecies").cast(IntegerType()))
           .withColumn("obsDt", F.get_json_object("raw_json", "$.obsDt"))
           .withColumn("obs_dt", F.to_timestamp("obsDt", "dd MMM yyyy HH:mm"))
           .withColumn("obs_date", F.to_date("obsDt", "dd MMM yyyy HH:mm"))
           .withColumn("locId", F.get_json_object("raw_json", "$.locId"))
           .withColumn("durationHrs", F.get_json_object("raw_json", "$.durationHrs").cast(DoubleType()))
           .withColumn("allObsReported", F.get_json_object("raw_json", "$.allObsReported").cast("boolean"))
           .withColumn("projId", F.get_json_object("raw_json", "$.projId"))
           .select(
               F.col("sub_id"),
               F.col("obs_dt"),
               F.col("obs_date"),
               F.col("numSpecies"),
               F.col("locId"),
               F.col("durationHrs"),
               F.col("allObsReported"),
               F.col("projId"),
               F.col("_run_id"),
               F.col("_ingestion_ts"),
               F.col("_run_date"),
               F.col("_source_file"),
               F.col("_fetch_error"),
           )
    )


# ── Silver: observations ──────────────────────────────────────────────────────
@dlt.table(
    name="observations_silver",
    comment="Individual bird observations - one row per species per checklist. Exploded from checklist detail.",
    table_properties={"quality": "silver"},
    partition_cols=["obs_date"],
)
@dlt.expect_or_drop("valid_species_code", "species_code IS NOT NULL")
@dlt.expect_or_drop("valid_obs_id",       "obs_id IS NOT NULL")
def observations_silver():
    return (
        dlt.read_stream("checklist_detail_bronze")
           .filter("_fetch_error IS NULL")
           # Extract checklist-level fields
           .withColumn("sub_id", F.get_json_object("raw_json", "$.subId"))
           .withColumn("obsDt", F.get_json_object("raw_json", "$.obsDt"))
           .withColumn("obs_dt", F.to_timestamp("obsDt", "dd MMM yyyy HH:mm"))
           .withColumn("obs_date", F.to_date("obsDt", "dd MMM yyyy HH:mm"))
           .withColumn("loc_id", F.get_json_object("raw_json", "$.locId"))
           # Extract observations array - using actual eBird API field names
           .withColumn("obs_array", F.from_json(
               F.get_json_object("raw_json", "$.obs"),
               "array<struct<speciesCode:string,obsId:string,howManyAtleast:int,howManyAtmost:int,howManyStr:string,present:boolean,exoticCategory:string>>"
           ))
           .withColumn("observation", F.explode("obs_array"))
           # Extract observation-level fields
           .select(
               F.col("sub_id"),
               F.col("obs_dt").alias("checklist_obs_dt"),
               F.col("obs_date"),
               F.col("loc_id"),
               F.col("observation.speciesCode").alias("species_code"),
               F.col("observation.obsId").alias("obs_id"),
               F.col("observation.howManyAtleast").alias("how_many_atleast"),
               F.col("observation.howManyAtmost").alias("how_many_atmost"),
               F.col("observation.howManyStr").alias("how_many_str"),
               F.col("observation.present").alias("present"),
               F.col("observation.exoticCategory").alias("exotic_category"),
               F.col("_run_id"),
               F.col("_ingestion_ts"),
               F.col("_run_date"),
               F.col("_source_file"),
           )
    )