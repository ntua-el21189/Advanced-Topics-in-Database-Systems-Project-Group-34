from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, lit
import pyspark.sql.functions as sf
from pyspark.sql.window import Window
from sedona.spark import *

# Έναρξη Spark Session
spark = SparkSession \
    .builder \
    .appName("Q5 solution") \
    .config("spark.executor.instances", "8") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .config("spark.jars", "/jars/sedona-spark-shaded-3.5_2.12-1.6.1.jar,/jars/geotools-wrapper-1.6.1-28.2.jar") \
    .getOrCreate()

# Ορισμός του Schema για τα Crimes
from pyspark.sql.functions import year, to_timestamp
crimes_schema = StructType([
    StructField("DR_NO", IntegerType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", IntegerType()),
    StructField("AREA", IntegerType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", IntegerType()),
    StructField("Part 1-2", IntegerType()),
    StructField("Crm Cd", IntegerType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", IntegerType()),
    StructField("Vict Age", IntegerType()),
    StructField("Vict Sex", StringType()),
    StructField("Vict_Descent", StringType()),
    StructField("Premis Cd", IntegerType()),
    StructField("Premis Desc", StringType()),
    StructField("Weapon Used Cd", IntegerType()),
    StructField("Weapon Desc", StringType()),
    StructField("Status", StringType()),
    StructField("Status Desc", StringType()),
    StructField("Crm Cd 1", IntegerType()),
    StructField("Crm Cd 2", IntegerType()),
    StructField("Crm Cd 3", IntegerType()),
    StructField("Crm Cd 4", IntegerType()),
    StructField("LOCATION", StringType()),
    StructField("Cross Street", StringType()),
    StructField("LAT", FloatType()),
    StructField("LON", FloatType())
])

crimes_df = spark.read.csv(
    "./data/LA_Crime_Data_2020_2025.csv",
    header=True,
    schema=crimes_schema
)\
                         .withColumnRenamed("DATE OCC", "DATE_OCC")
crimes_df = crimes_df.select(col("DR_NO"),col("DATE_OCC"),col("LAT"), col("LON"))
crimes_df_year = crimes_df.withColumn("DATE_OCC", sf.year(to_timestamp("DATE_OCC", 
                "yyyy MMM dd hh:mm:ss a"))) 
crimes_df= crimes_df_year.withColumnRenamed("DATE_OCC", "Year")

# Κρατάμε μόνο 2020 και 2021
crimes_df = crimes_df.filter(col("Year").isin([2020, 2021]))

# Φιλτράρουμε null συντεταγμένες
crimes_df = crimes_df.filter(
    (col("LAT").isNotNull()) &
    (col("LON").isNotNull())
)
# Έλεγχος ότι έμειναν μόνο αυτά τα έτη
print("Years included:")
crimes_df.select("Year").distinct().show()
crimes_df.printSchema()

income_schema = StructType([
    StructField("Zip Code", IntegerType()),
    StructField("Community", StringType()),
    StructField("Estimated Median Income", StringType())
])
income_df1 = spark.read.csv(
    "./data/LA_income_2021.csv",
    sep=";",
    header=True,
    schema=income_schema,
)\
                         .withColumnRenamed("Zip Code", "Zip_Code")
# Κάνω το string integer
income_df= income_df1.withColumn("Estimated Median Income",sf.regexp_replace("Estimated Median Income", "[$,]", "").cast("int"))
income_df.printSchema()

# Διαβάζω το geojson απο το S3
# Create sedona context
sedona = SedonaContext.create(spark)
# Read the file from s3
geojson_path = "./data/LA_Census_Blocks_2020.geojson"
blocks_df = sedona.read.format("geojson") \
            .option("multiLine", "true").load(geojson_path) \
            .selectExpr("explode(features) as features") \
            .select("features.*")
# Formatting magic
blocks_df = blocks_df.select( \
                [col(f"properties.{col_name}").alias(col_name) for col_name in \
                blocks_df.schema["properties"].dataType.fieldNames()] + ["geometry"]) \
            .drop("properties") \
            .drop("type")
blocks_df=blocks_df.filter(col("CITY") == "Los Angeles")
# Print schema
blocks_df.printSchema()

print("============================================================================================")
# Κάνω geometry τις συντεταγμένες του crimes για να μπορέσω να τα τοποθετήσω στa blocks 
# Convert to geometry
crimes_df = crimes_df.withColumn(
    "crime_geom",
    sf.expr("ST_Point(cast(LON as double), cast(LAT as double))") 
)
print(crimes_df.show(5))
# Join blocks with income data
import time
start_time_join_inc=time.time()
blocks_and_income = blocks_df.join(
    income_df,
    income_df.Zip_Code == blocks_df.ZCTA20
).withColumn("Income_per_block", col("HOUSING20") * col("Estimated Median Income"))\
    .select("COMM", "POP20", "Income_per_block", "geometry", "ZCTA20","CITY")
blocks_and_income.show()
end_time_join_inc=time.time()
print(f"Execution Time (DataFrame API): {end_time_join_inc - start_time_join_inc:.2f} seconds")
blocks_and_income.printSchema()

print("============================================================================================")   
# Join στον τόπο του εγκλήματος και το block στο οποίο ανήκει
# join condition here is whether the geometry defined in df1.geom is contained
# within flattened_df.geometry.

from pyspark.sql.functions import count,first
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum
import time


import time
start_time_join = time.time()
crimes_in_block_with_inc = crimes_df \
    .join(blocks_and_income, ST_Within(crimes_df.crime_geom, blocks_and_income.geometry))\
    .groupby(blocks_and_income.geometry).agg(#  group by geom to get the blocks
        first("CITY").alias("CITY"),
        first("Income_per_block").alias("Income_per_block"),
        first("POP20").alias("block_population"),
        first("COMM").alias("COMM"),  # block comm name
        count("*").alias("crime_count")
        )
crimes_in_block_with_inc.show()
end_time_join = time.time()
print(f"Execution Time (join): {end_time_join - start_time_join:.2f} seconds")
crimes_in_block_with_inc.explain(mode="formatted")

print("============================================================================================")

from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

LA_areas_final = crimes_in_block_with_inc \
    .groupBy("COMM").agg(
        # ST_Union_Aggr("geometry").alias("geometry"),
        spark_sum("block_population").alias("total_population"),
        spark_sum("crime_count").alias("total_crime_count"),
        spark_sum("Income_per_block").alias("total_income")
    ) \
    .withColumn("crimes_per_person", col("total_crime_count") / col("total_population")) \
    .withColumn("income_per_capita", col("total_income") / col("total_population"))
LA_areas_final.printSchema()

# Θα πρέπει να καταλήξω σε κάτι τέτοιο 
#clean_stats = final.filter(col("total_population") > 1000)##
start_time_cor=time.time()
correlation = LA_areas_final.stat.corr("income_per_capita", "crimes_per_person")
print("Correlation =", correlation)
end_time_cor=time.time()
print(f"Execution Time (cor) : {end_time_cor - start_time_cor:.2f} seconds")

start_time_cor_high=time.time()
lowest10 = LA_areas_final.orderBy(col("income_per_capita").asc()).limit(10)
highest10 = LA_areas_final.orderBy(col("income_per_capita").desc()).limit(10)
subset = lowest10.union(highest10)
correlation = subset.stat.corr("Income_per_Capita", "Crimes_per_Person")
print("Correlation =", correlation)
end_time_cor_high=time.time()
print(f"Execution Time (cor_high): {end_time_cor_high - start_time_cor_high:.2f} seconds")
