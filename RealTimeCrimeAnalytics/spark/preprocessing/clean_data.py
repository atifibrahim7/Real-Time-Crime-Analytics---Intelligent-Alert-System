import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col, to_timestamp, to_date

def create_spark_session():
    return SparkSession.builder \
        .appName("Chicago Crime Data Batch Processing") \
        .getOrCreate()

def process_arrests(spark, input_path, output_path):
    schema = StructType([
        StructField("cb_no", StringType(), True),
        StructField("case_number", StringType(), True),
        StructField("arrest_date", StringType(), True),
        StructField("race", StringType(), True),
        StructField("charge_1_statute", StringType(), True),
        StructField("charge_1_description", StringType(), True),
        StructField("charge_1_type", StringType(), True),
        StructField("charge_1_class", StringType(), True),
        StructField("charge_2_statute", StringType(), True),
        StructField("charge_2_description", StringType(), True),
        StructField("charge_2_type", StringType(), True),
        StructField("charge_2_class", StringType(), True),
        StructField("charge_3_statute", StringType(), True),
        StructField("charge_3_description", StringType(), True),
        StructField("charge_3_type", StringType(), True),
        StructField("charge_3_class", StringType(), True),
        StructField("charge_4_statute", StringType(), True),
        StructField("charge_4_description", StringType(), True),
        StructField("charge_4_type", StringType(), True),
        StructField("charge_4_class", StringType(), True),
        StructField("charges_statute", StringType(), True),
        StructField("charges_description", StringType(), True),
        StructField("charges_type", StringType(), True),
        StructField("charges_class", StringType(), True)
    ])
    
    df = spark.read.csv(input_path, header=True, schema=schema, multiLine=True, escape='"')
    
    # Cast timestamp
    # "2026-05-01T23:50:00.000" -> yyyy-MM-dd'T'HH:mm:ss.SSS
    df = df.withColumn("arrest_date", to_timestamp(col("arrest_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
    
    df.write.parquet(output_path, mode="overwrite")
    print("Processed arrests successfully.")

def process_crimes(spark, input_path, output_path):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("case_number", StringType(), True),
        StructField("date", StringType(), True),
        StructField("block", StringType(), True),
        StructField("iucr", StringType(), True),
        StructField("primary_type", StringType(), True),
        StructField("description", StringType(), True),
        StructField("location_description", StringType(), True),
        StructField("arrest", StringType(), True),
        StructField("domestic", StringType(), True),
        StructField("beat", StringType(), True),
        StructField("district", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("community_area", StringType(), True),
        StructField("fbi_code", StringType(), True),
        StructField("x_coordinate", DoubleType(), True),
        StructField("y_coordinate", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("updated_on", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True)
    ])
    
    df = spark.read.csv(input_path, header=True, schema=schema, multiLine=True, escape='"')
    
    df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
           .withColumn("updated_on", to_timestamp(col("updated_on"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
           .withColumn("arrest", col("arrest").cast("boolean")) \
           .withColumn("domestic", col("domestic").cast("boolean"))
           
    df.write.parquet(output_path, mode="overwrite")
    print("Processed crimes successfully.")

def process_police_stations(spark, input_path, output_path):
    schema = StructType([
        StructField("district", StringType(), True),
        StructField("district_name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("website", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("fax", StringType(), True),
        StructField("tty", StringType(), True),
        StructField("x_coordinate", DoubleType(), True),
        StructField("y_coordinate", DoubleType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True)
    ])
    
    df = spark.read.csv(input_path, header=True, schema=schema, multiLine=True, escape='"')
    df.write.parquet(output_path, mode="overwrite")
    print("Processed police stations successfully.")

def process_sex_offenders(spark, input_path, output_path):
    schema = StructType([
        StructField("last", StringType(), True),
        StructField("first", StringType(), True),
        StructField("block", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("race", StringType(), True),
        StructField("birth_date", StringType(), True),
        StructField("height", IntegerType(), True),
        StructField("weight", IntegerType(), True),
        StructField("victim_minor", StringType(), True)
    ])
    
    df = spark.read.csv(input_path, header=True, schema=schema, multiLine=True, escape='"')
    
    # "06/19/1970"
    df = df.withColumn("birth_date", to_date(col("birth_date"), "MM/dd/yyyy"))
    
    df.write.parquet(output_path, mode="overwrite")
    print("Processed sex offenders successfully.")

def process_violence(spark, input_path, output_path):
    schema = StructType([
        StructField("case_number", StringType(), True),
        StructField("date", StringType(), True),
        StructField("block", StringType(), True),
        StructField("victimization_primary", StringType(), True),
        StructField("incident_primary", StringType(), True),
        StructField("gunshot_injury_i", StringType(), True),
        StructField("unique_id", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("ward", StringType(), True),
        StructField("community_area", StringType(), True),
        StructField("street_outreach_organization", StringType(), True),
        StructField("area", StringType(), True),
        StructField("district", StringType(), True),
        StructField("beat", StringType(), True),
        StructField("age", StringType(), True),
        StructField("sex", StringType(), True),
        StructField("race", StringType(), True),
        StructField("victimization_fbi_cd", StringType(), True),
        StructField("incident_fbi_cd", StringType(), True),
        StructField("victimization_fbi_descr", StringType(), True),
        StructField("incident_fbi_descr", StringType(), True),
        StructField("victimization_iucr_cd", StringType(), True),
        StructField("incident_iucr_cd", StringType(), True),
        StructField("victimization_iucr_secondary", StringType(), True),
        StructField("incident_iucr_secondary", StringType(), True),
        StructField("homicide_victim_first_name", StringType(), True),
        StructField("homicide_victim_mi", StringType(), True),
        StructField("homicide_victim_last_name", StringType(), True),
        StructField("month", IntegerType(), True),
        StructField("day_of_week", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("location_description", StringType(), True),
        StructField("state_house_district", StringType(), True),
        StructField("state_senate_district", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("location", StringType(), True)
    ])
    
    df = spark.read.csv(input_path, header=True, schema=schema, multiLine=True, escape='"')
    
    df = df.withColumn("date", to_timestamp(col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS")) \
           .withColumn("updated", to_timestamp(col("updated"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
           
    df.write.parquet(output_path, mode="overwrite")
    print("Processed violence data successfully.")

if __name__ == "__main__":
    spark = create_spark_session()
    
    # We are now in spark/preprocessing/ but the base_dir is still the same absolute path
    # Use /data if running inside the container, else fallback to local path
    if os.path.exists("/data"):
        base_dir = "/data"
    else:
        base_dir = "/home/ali/Documents/BigData/RealTimeCrimeAnalytics/data"
        
    cleaned_dir = os.path.join(base_dir, "cleaned")
    os.makedirs(cleaned_dir, exist_ok=True)
    
    process_arrests(spark, os.path.join(base_dir, "arrests.csv"), os.path.join(cleaned_dir, "arrests.parquet"))
    process_crimes(spark, os.path.join(base_dir, "crimes.csv"), os.path.join(cleaned_dir, "crimes.parquet"))
    process_police_stations(spark, os.path.join(base_dir, "police_stations.csv"), os.path.join(cleaned_dir, "police_stations.parquet"))
    process_sex_offenders(spark, os.path.join(base_dir, "sex_offenders.csv"), os.path.join(cleaned_dir, "sex_offenders.parquet"))
    process_violence(spark, os.path.join(base_dir, "violence.csv"), os.path.join(cleaned_dir, "violence.parquet"))
    
    spark.stop()
    print("All datasets processed and saved to Parquet successfully.")
