from pyspark.sql import functions as F
import argparse
from databricks.sdk.runtime import spark

# Process command-line arguments
parser = argparse.ArgumentParser(
    description="Databricks job with catalog and schema parameters",
)
parser.add_argument("--catalog_source", required=True)
parser.add_argument("--catalog_target", required=True)
parser.add_argument("--schema_target", required=True)
args = parser.parse_args()

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {args.catalog_target}.{args.schema_target}")

bookings = spark.table(f"{args.catalog_source}.wanderbricks.bookings")
properties = spark.table(f"{args.catalog_source}.wanderbricks.properties")
destinations = spark.table(f"{args.catalog_source}.wanderbricks.destinations")

revenue_by_dest = (
    bookings.filter(F.col("status") == "completed")
    .join(properties, "property_id")
    .join(destinations, "destination_id")
    .groupBy("destination", "country")
    .agg(F.sum("total_amount").alias("total_revenue"),
        F.count("booking_id").alias("booking_count"),
        # Lab 1: add a new metric here, then redeploy and rerun the job.
        F.avg("total_amount").alias("avg_booking_value")
        )
    .orderBy(F.desc("total_revenue"))
)

revenue_by_dest.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(f"{args.catalog_target}.{args.schema_target}.revenue_by_destination")
