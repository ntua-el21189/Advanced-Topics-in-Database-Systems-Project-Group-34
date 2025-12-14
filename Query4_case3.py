#Fix the configurations for the spark executors

from pyspark import SparkConf, SparkContext
conf = SparkConf()
conf.set('spark.executor.memory', '8g')
conf.set('spark.executor.cores', '4')
conf.set('spark.executor.instances', '2')   


#Υλοποίηση με DataFrames
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col
from sedona.spark import *


spark = SparkSession.builder \
    .appName("Q4_case3") \
    .config(conf=conf) \
    .config("spark.driver.memory", "6g") \
    .config("spark.jars", "/jars/sedona-spark-shaded-3.5_2.12-1.6.1.jar,/jars/geotools-wrapper-1.6.1-28.2.jar") \
    .getOrCreate()
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
    StructField("Vict Descent", StringType()),
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
    StructField("LOCATION",StringType()),
    StructField("Cross Street",StringType()),
    StructField("LAT",FloatType()),
    StructField("LON",FloatType())
])

crimes_df1 = spark.read.csv("./data/LA_Crime_Data_2010_2019.csv", \
                         header=True, \
                         schema= crimes_schema)

crimes_df1 = crimes_df1.select(col("DR_NO"), col("LAT"), col("LON"))

crimes_df2 =  spark.read.csv("./data/LA_Crime_Data_2020_2025.csv", \
                         header=True, \
                         schema= crimes_schema)
crimes_df2 = crimes_df2.select(col("DR_NO"), col("LAT"), col("LON"))

crimes_df = crimes_df1.union(crimes_df2)
crimes_df = crimes_df.filter((col("LAT") != 0) & (col("LON") != 0) & col("LAT").isNotNull() & col("LON").isNotNull())

stations_schema = StructType([
    StructField("x", FloatType()),
    StructField("y", FloatType()),
    StructField("FID", IntegerType()),
    StructField("DIVISION", StringType()),
    StructField("LOCATION", StringType()),
    StructField("PREC", IntegerType())
    
])

stations_df = spark.read.csv("./data/LA_Police_Stations.csv",
                        header=True,
                        schema=stations_schema)
stations_df = stations_df.select(col("x"), col("y"), col("DIVISION"))

print(crimes_df.show(5))
print("===============================================================")

from pyspark.sql.functions import col
from pyspark.sql.functions import expr
sedona = SedonaContext.create(spark)

# Convert to geometry
crimes_df = crimes_df.withColumn(
    "crime_geom",
    expr("ST_Point(cast(LON as double), cast(LAT as double))") 
)

# Convert longitude/latitude to geometry
stations_df = stations_df.withColumn(
    "station_geom",
    expr("ST_Point(cast(x as double), cast(y as double))") 
)

print(crimes_df.show(5))
print(stations_df.show(5))

print("===============================================================")

from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number
from pyspark.sql.functions import count, avg
import time

start_time_df = time.time()

joined = crimes_df.crossJoin(stations_df).select(col("DR_NO"),col("crime_geom"),col("DIVISION"),col("station_geom"))

joined = joined.withColumn("distance_km", expr("ST_DistanceSphere(station_geom, crime_geom)")/1000) 

# Window: μία ομάδα για κάθε DR_NO, ταξινόμηση με βάση distance
w = Window.partitionBy("DR_NO").orderBy(col("distance_km").asc())

nearest = joined.withColumn("rn", row_number().over(w)).filter(col("rn") == 1).drop("rn")

result = nearest.groupBy("DIVISION").agg(avg("distance_km").alias("average_distance"),count("*").alias("number_of_crimes")).orderBy(col("number_of_crimes").desc())
print(result.show())
print(stations_df.show(5))

end_time_df = time.time()
print(f"Execution Time (Data Frames API): {end_time_df - start_time_df:.2f} seconds")

result.explain(mode="formatted")

print("===============================================================")

#SQL API
crimes_df.createOrReplaceTempView("crimes")
stations_df.createOrReplaceTempView("stations")

start_time_sql = time.time()

joined_query = "SELECT \
            c.DR_NO, \
            c.crime_geom, \
            s.DIVISION, \
            s.station_geom, \
            ST_DistanceSphere(c.crime_geom, s.station_geom)/1000 AS distance_km \
        FROM crimes c \
        CROSS JOIN stations s"

joined_df = spark.sql(joined_query)
joined_df.createOrReplaceTempView("joined")

nearest_query = "SELECT * \
        FROM ( \
            SELECT *, \
            ROW_NUMBER() OVER (PARTITION BY DR_NO ORDER BY distance_km ASC) AS rn \
            FROM joined ) \
        WHERE rn = 1"

nearest_df = spark.sql(nearest_query)
nearest_df.createOrReplaceTempView("nearest")

results_query = "SELECT \
        DIVISION, \
        AVG(distance_km) AS avg_distance, \
        COUNT(*) AS num_crimes \
        FROM nearest \
        GROUP BY DIVISION \
        ORDER BY num_crimes DESC"

results_df = spark.sql(results_query)

print(results_df.show())
end_time_sql = time.time()
print(f"Execution Time (SQL API): {end_time_sql - start_time_sql:.2f} seconds")
results_df.explain(mode="formatted")

print("===============================================================")