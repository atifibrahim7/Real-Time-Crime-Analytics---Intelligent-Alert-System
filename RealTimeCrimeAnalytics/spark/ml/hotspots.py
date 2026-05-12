"""
hotspots.py — Batch Layer ML: Crime Hotspot Detection
======================================================
Uses PySpark MLlib KMeans to cluster crimes geospatially and identify
the top crime hotspots for police resource allocation (Section 7.5).

Results (cluster centroids) are written to PostgreSQL.

Usage (inside Spark Master container):
    /opt/spark/bin/spark-submit \
        --jars /tmp/postgresql-42.7.3.jar \
        /spark/ml/hotspots.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


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
NUM_CLUSTERS = 10


def create_spark_session():
    """Create a SparkSession for the ML job."""
    return SparkSession.builder \
        .appName("Chicago Crime Hotspot Detection (KMeans)") \
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


if __name__ == "__main__":
    spark = create_spark_session()

    # ──────────────────────────────────────────
    # Step 1: Load and filter the crimes dataset
    # ──────────────────────────────────────────
    print("\n[Step 1] Loading crimes.parquet …")
    crimes = spark.read.parquet(f"{PARQUET_DIR}/crimes.parquet")

    # Filter out rows with null latitude or longitude
    crimes_geo = crimes.filter(
        F.col("latitude").isNotNull() & F.col("longitude").isNotNull()
    ).select("id", "primary_type", "district", "latitude", "longitude")

    total_records = crimes_geo.count()
    print(f"  ✓ {total_records} geo-located crime records loaded.\n")

    # ──────────────────────────────────────────
    # Step 2: Assemble feature vector
    # ──────────────────────────────────────────
    print("[Step 2] Assembling feature vector (latitude, longitude) …")
    assembler = VectorAssembler(
        inputCols=["latitude", "longitude"],
        outputCol="features"
    )
    crimes_features = assembler.transform(crimes_geo)
    print("  ✓ Feature vector assembled.\n")

    # ──────────────────────────────────────────
    # Step 3: Train KMeans model (k=10)
    # ──────────────────────────────────────────
    print(f"[Step 3] Training KMeans with k={NUM_CLUSTERS} …")
    kmeans = KMeans(k=NUM_CLUSTERS, seed=42, featuresCol="features", predictionCol="cluster")
    model = kmeans.fit(crimes_features)

    # Evaluate clustering quality using Within Set Sum of Squared Errors
    wssse = model.summary.trainingCost
    print(f"  ✓ KMeans trained. WSSSE = {wssse:.4f}\n")

    # ──────────────────────────────────────────
    # Step 4: Extract cluster labels and centroids
    # ──────────────────────────────────────────
    print("[Step 4] Extracting cluster centroids and labels …")

    # Add cluster labels to the original data
    predictions = model.transform(crimes_features)

    # Count crimes per cluster for context
    cluster_counts = predictions.groupBy("cluster").agg(
        F.count("*").alias("crime_count")
    ).orderBy("cluster")

    # Extract centroids as a DataFrame
    centroids = model.clusterCenters()
    centroid_rows = []
    for idx, center in enumerate(centroids):
        centroid_rows.append((idx, float(center[0]), float(center[1])))

    centroids_df = spark.createDataFrame(
        centroid_rows,
        ["cluster_id", "centroid_latitude", "centroid_longitude"]
    )

    # Join centroids with their crime counts
    centroids_with_counts = centroids_df.join(
        cluster_counts,
        centroids_df.cluster_id == cluster_counts.cluster,
        how="left"
    ).drop("cluster").orderBy(F.col("crime_count").desc())

    centroids_with_counts.show(truncate=False)

    # ──────────────────────────────────────────
    # Step 5: Write results to PostgreSQL
    # ──────────────────────────────────────────
    print("[Step 5] Writing results to PostgreSQL …")

    # Write centroids table (primary output for section 7.5)
    write_to_postgres(centroids_with_counts, "hotspots")

    # Also write the labeled crimes for downstream use (e.g., dashboard maps)
    labeled_crimes = predictions.select(
        "id", "primary_type", "district", "latitude", "longitude", "cluster"
    )
    write_to_postgres(labeled_crimes, "crimes_with_clusters")

    spark.stop()
    print("\n✅ Hotspot Detection complete — centroids and labels written to PostgreSQL.")
