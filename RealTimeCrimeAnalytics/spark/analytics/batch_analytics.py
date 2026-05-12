"""
batch_analytics.py — Batch Layer Core Analytics
=================================================
Reads five preprocessed Parquet datasets and computes business intelligence
metrics mandated by project sections 7.1–7.4 and 7.6.

Results are written to PostgreSQL (the Serving Layer) via JDBC.

Usage (inside Spark Master container):
    /opt/spark/bin/spark-submit \
        --jars /tmp/postgresql-42.7.3.jar \
        /spark/analytics/batch_analytics.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────
PARQUET_DIR = "/data/cleaned"
JDBC_URL = "jdbc:postgresql://postgres:5432/crime_db"
JDBC_PROPS = {
    "user": "admin",
    "password": "admin",
    "driver": "org.postgresql.Driver"
}


def create_spark_session():
    """Create a SparkSession for Batch Analytics."""
    return SparkSession.builder \
        .appName("Chicago Crime Batch Analytics") \
        .config("spark.jars", "/tmp/postgresql-42.7.3.jar") \
        .getOrCreate()


def write_to_postgres(df, table_name):
    """Helper: write a DataFrame to PostgreSQL with overwrite mode."""
    df.write.jdbc(
        url=JDBC_URL,
        table=table_name,
        mode="overwrite",
        properties=JDBC_PROPS
    )
    print(f"  ✓ Written to PostgreSQL table: {table_name}")


# ─────────────────────────────────────────────
# 7.1  Crime Trends
# ─────────────────────────────────────────────
def compute_crime_trends(crimes):
    """
    Group crime counts by year, month, day-of-week, and hour-of-day.
    Produces a single wide table that captures temporal crime patterns.
    """
    print("\n[1/5] Computing Crime Trends …")

    # Extract temporal components from the 'date' timestamp column
    trends = crimes.select(
        F.year("date").alias("year"),
        F.month("date").alias("month"),
        F.dayofweek("date").alias("day_of_week"),
        F.hour("date").alias("hour_of_day")
    )

    # --- Crimes by Year ---
    by_year = trends.groupBy("year") \
        .agg(F.count("*").alias("crime_count")) \
        .orderBy("year")
    write_to_postgres(by_year, "crime_trends_by_year")

    # --- Crimes by Month ---
    by_month = trends.groupBy("year", "month") \
        .agg(F.count("*").alias("crime_count")) \
        .orderBy("year", "month")
    write_to_postgres(by_month, "crime_trends_by_month")

    # --- Crimes by Day of Week (1=Sunday … 7=Saturday) ---
    by_dow = trends.groupBy("day_of_week") \
        .agg(F.count("*").alias("crime_count")) \
        .orderBy("day_of_week")
    write_to_postgres(by_dow, "crime_trends_by_day_of_week")

    # --- Crimes by Hour of Day ---
    by_hour = trends.groupBy("hour_of_day") \
        .agg(F.count("*").alias("crime_count")) \
        .orderBy("hour_of_day")
    write_to_postgres(by_hour, "crime_trends_by_hour")

    print("  ✓ Crime Trends complete.")


# ─────────────────────────────────────────────
# 7.2  Arrest Rates
# ─────────────────────────────────────────────
def compute_arrest_rates(crimes, arrests):
    """
    Join Crimes ↔ Arrests on case_number.
    Calculate arrest rates by primary crime type, district, and race.
    Identify the top-10 crime types by arrest rate.
    """
    print("\n[2/5] Computing Arrest Rates …")

    # Join crimes with arrests on case_number
    joined = crimes.join(
        arrests.select("case_number", "race").withColumnRenamed("race", "arrestee_race"),
        on="case_number",
        how="left"
    )

    # Flag: was there a matching arrest record?
    joined = joined.withColumn(
        "has_arrest",
        F.when(F.col("arrestee_race").isNotNull(), 1).otherwise(0)
    )

    # --- Arrest rate by Primary Crime Type ---
    by_type = joined.groupBy("primary_type").agg(
        F.count("*").alias("total_crimes"),
        F.sum("has_arrest").alias("total_arrests")
    ).withColumn(
        "arrest_rate", F.round(F.col("total_arrests") / F.col("total_crimes"), 4)
    ).orderBy(F.col("arrest_rate").desc())

    write_to_postgres(by_type, "arrest_rate_by_crime_type")

    # --- Top 10 crime types by arrest rate (min 10 crimes to avoid noise) ---
    top10 = by_type.filter(F.col("total_crimes") >= 10) \
        .orderBy(F.col("arrest_rate").desc()) \
        .limit(10)
    write_to_postgres(top10, "top10_crime_types_by_arrest_rate")

    # --- Arrest rate by District ---
    by_district = joined.groupBy("district").agg(
        F.count("*").alias("total_crimes"),
        F.sum("has_arrest").alias("total_arrests")
    ).withColumn(
        "arrest_rate", F.round(F.col("total_arrests") / F.col("total_crimes"), 4)
    ).orderBy("district")
    write_to_postgres(by_district, "arrest_rate_by_district")

    # --- Arrest rate by Arrestee Race ---
    by_race = joined.filter(F.col("arrestee_race").isNotNull()) \
        .groupBy("arrestee_race").agg(
            F.count("*").alias("total_arrests"),
        ).orderBy(F.col("total_arrests").desc())
    write_to_postgres(by_race, "arrests_by_race")

    print("  ✓ Arrest Rates complete.")


# ─────────────────────────────────────────────
# 7.3  Violence & Gunshot Analysis
# ─────────────────────────────────────────────
def compute_violence_analysis(violence):
    """
    Compute homicides vs. non-fatal shootings by month and district.
    Calculate the proportion of incidents where GUNSHOT_INJURY_I = 'YES'.
    """
    print("\n[3/5] Computing Violence & Gunshot Analysis …")

    # --- Homicides vs. Non-Fatal by Month ---
    by_month = violence.groupBy("month", "victimization_primary").agg(
        F.count("*").alias("incident_count")
    ).orderBy("month", "victimization_primary")
    write_to_postgres(by_month, "violence_by_month")

    # --- Homicides vs. Non-Fatal by District ---
    by_district = violence.groupBy("district", "victimization_primary").agg(
        F.count("*").alias("incident_count")
    ).orderBy("district", "victimization_primary")
    write_to_postgres(by_district, "violence_by_district")

    # --- Gunshot Injury Proportion ---
    gunshot_stats = violence.agg(
        F.count("*").alias("total_incidents"),
        F.sum(F.when(F.col("gunshot_injury_i") == "YES", 1).otherwise(0)).alias("gunshot_yes"),
    ).withColumn(
        "gunshot_proportion", F.round(F.col("gunshot_yes") / F.col("total_incidents"), 4)
    )
    write_to_postgres(gunshot_stats, "gunshot_injury_proportion")

    # --- Gunshot proportion by district for deeper insight ---
    gunshot_by_district = violence.groupBy("district").agg(
        F.count("*").alias("total_incidents"),
        F.sum(F.when(F.col("gunshot_injury_i") == "YES", 1).otherwise(0)).alias("gunshot_yes"),
    ).withColumn(
        "gunshot_proportion", F.round(F.col("gunshot_yes") / F.col("total_incidents"), 4)
    ).orderBy(F.col("gunshot_proportion").desc())
    write_to_postgres(gunshot_by_district, "gunshot_proportion_by_district")

    # --- Top Community Areas by Violence Incidence ---
    top_community_areas = violence.groupBy("community_area").agg(
        F.count("*").alias("total_incidents"),
        F.sum(F.when(F.col("victimization_primary") == "HOMICIDE", 1).otherwise(0)).alias("homicides"),
        F.sum(F.when(F.col("victimization_primary") != "HOMICIDE", 1).otherwise(0)).alias("non_fatal")
    ).filter(
        F.col("community_area").isNotNull()
    ).orderBy(F.col("total_incidents").desc())
    write_to_postgres(top_community_areas, "top_community_areas_by_violence")

    print("  ✓ Violence & Gunshot Analysis complete.")


# ─────────────────────────────────────────────
# 7.4  Sex Offender Proximity (by District)
# ─────────────────────────────────────────────
def compute_sex_offender_proximity(sex_offenders, crimes, police_stations):
    """
    Sex offenders do not have a 'district' column directly, so we build
    a block → district lookup from the Crimes dataset and use it to map
    each offender to a police district.  Then we join with Police Stations
    to identify districts with the highest density of offenders.
    """
    print("\n[4/5] Computing Sex Offender Proximity …")

    # Build a block → district lookup from crimes (take most frequent district per block)
    block_district_lookup = crimes \
        .filter(F.col("block").isNotNull() & F.col("district").isNotNull()) \
        .groupBy("block", "district") \
        .agg(F.count("*").alias("cnt"))

    # Window to pick the most common district for each block
    w = Window.partitionBy("block").orderBy(F.col("cnt").desc())
    block_district_lookup = block_district_lookup \
        .withColumn("rank", F.row_number().over(w)) \
        .filter(F.col("rank") == 1) \
        .select("block", "district")

    # Map sex offenders to districts via block
    offenders_with_district = sex_offenders.join(
        block_district_lookup,
        on="block",
        how="left"
    )

    # Count offenders per district
    offender_density = offenders_with_district \
        .filter(F.col("district").isNotNull()) \
        .groupBy("district") \
        .agg(F.count("*").alias("offender_count")) \
        .orderBy(F.col("offender_count").desc())

    # Join with police stations for station details
    offender_proximity = offender_density.join(
        police_stations.select("district", "district_name", "address"),
        on="district",
        how="left"
    ).orderBy(F.col("offender_count").desc())

    write_to_postgres(offender_proximity, "sex_offender_density_by_district")

    # --- Flag records where VICTIM_MINOR = 'Y' for priority reporting ---
    minor_victims = sex_offenders.filter(
        F.col("victim_minor") == "Y"
    ).select(
        "last", "first", "block", "gender", "race", "birth_date",
        "height", "weight", "victim_minor"
    )
    write_to_postgres(minor_victims, "sex_offenders_minor_victims_priority")

    print("  ✓ Sex Offender Proximity complete.")


# ─────────────────────────────────────────────
# 7.6  Cross-Dataset Correlations
# ─────────────────────────────────────────────
def compute_correlations(crimes, arrests, violence, sex_offenders):
    """
    Compute at least two cross-dataset correlations:
    1. Violence rate vs. total arrest rate grouped by district.
    2. Sex offender density vs. crime rate grouped by community area.
    """
    print("\n[5/5] Computing Cross-Dataset Correlations …")

    # ── Correlation 1: Violence rate vs. Arrest rate by District ──
    crime_by_district = crimes.groupBy("district").agg(
        F.count("*").alias("total_crimes")
    )

    arrest_by_district = crimes.filter(F.col("arrest") == True).groupBy("district").agg(
        F.count("*").alias("total_arrests")
    )

    violence_by_district = violence.groupBy("district").agg(
        F.count("*").alias("total_violence_incidents")
    )

    corr1 = crime_by_district \
        .join(arrest_by_district, on="district", how="left") \
        .join(violence_by_district, on="district", how="left") \
        .fillna(0, subset=["total_arrests", "total_violence_incidents"])

    corr1 = corr1.withColumn(
        "arrest_rate", F.round(F.col("total_arrests") / F.col("total_crimes"), 4)
    ).withColumn(
        "violence_rate", F.round(F.col("total_violence_incidents") / F.col("total_crimes"), 4)
    ).orderBy("district")

    write_to_postgres(corr1, "district_arrest_vs_violence_correlation")

    # ── Correlation 2: Sex offender density vs. Crime rate by Community Area ──
    crime_by_community = crimes.filter(
        F.col("community_area").isNotNull()
    ).groupBy("community_area").agg(
        F.count("*").alias("total_crimes")
    )

    # Map sex offenders to community areas via block lookup from crimes
    block_community_lookup = crimes \
        .filter(F.col("block").isNotNull() & F.col("community_area").isNotNull()) \
        .groupBy("block", "community_area") \
        .agg(F.count("*").alias("cnt"))

    w = Window.partitionBy("block").orderBy(F.col("cnt").desc())
    block_community_lookup = block_community_lookup \
        .withColumn("rank", F.row_number().over(w)) \
        .filter(F.col("rank") == 1) \
        .select("block", "community_area")

    offenders_with_community = sex_offenders.join(
        block_community_lookup, on="block", how="left"
    )

    offender_by_community = offenders_with_community \
        .filter(F.col("community_area").isNotNull()) \
        .groupBy("community_area") \
        .agg(F.count("*").alias("offender_count"))

    corr2 = crime_by_community \
        .join(offender_by_community, on="community_area", how="left") \
        .fillna(0, subset=["offender_count"]) \
        .withColumn(
            "offender_density_per_100_crimes",
            F.round((F.col("offender_count") / F.col("total_crimes")) * 100, 4)
        ).orderBy(F.col("total_crimes").desc())

    write_to_postgres(corr2, "community_offender_vs_crime_correlation")

    print("  ✓ Cross-Dataset Correlations complete (2 correlations).")


# ─────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────
if __name__ == "__main__":
    spark = create_spark_session()

    # Load all five cleaned Parquet datasets
    print("Loading Parquet datasets …")
    crimes          = spark.read.parquet(f"{PARQUET_DIR}/crimes.parquet")
    arrests         = spark.read.parquet(f"{PARQUET_DIR}/arrests.parquet")
    police_stations = spark.read.parquet(f"{PARQUET_DIR}/police_stations.parquet")
    violence        = spark.read.parquet(f"{PARQUET_DIR}/violence.parquet")
    sex_offenders   = spark.read.parquet(f"{PARQUET_DIR}/sex_offenders.parquet")
    print("  ✓ All datasets loaded.\n")

    # Execute each analytics module
    compute_crime_trends(crimes)
    compute_arrest_rates(crimes, arrests)
    compute_violence_analysis(violence)
    compute_sex_offender_proximity(sex_offenders, crimes, police_stations)
    compute_correlations(crimes, arrests, violence, sex_offenders)

    spark.stop()
    print("\n✅ Batch Analytics complete — all results written to PostgreSQL.")
