from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,TimestampNTZType,StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Q2 DF implementation") \
    .config("spark.executor.instances", "4") \
    .config("spark.executor.memory", "2g") \
    .config("spark.executor.cores", "1") \
    .getOrCreate()

#Υλοποίηση με RDDs
from pyspark.sql import SparkSession
import csv
import time

sc = spark.sparkContext

rdd1 = sc.textFile("./data/LA_Crime_Data_2010_2019.csv")\
    .map(lambda line: next(csv.reader([line])))\
    .map(lambda x: [x[0], x[10]]) 

rdd2 = sc.textFile("./data/LA_Crime_Data_2020_2025.csv")\
    .map(lambda line: next(csv.reader([line])))\
    .map(lambda x: [x[0], x[10]]) 

rdd3 = sc.textFile("./data/MO_codes.txt")\
    .map(lambda line: next(csv.reader([line]))) 

rdd = rdd1.union(rdd2)
header = rdd.first()
rdd = rdd.filter(lambda x: x!=header)

rdd.take(5)

rdd3 = rdd3.map(lambda x: (x[0].split(" ", 1)[0], x[0].split(" ", 1)[1]))

rdd3.take(5)

import time 
start_time_rdd = time.time()

rdd_tuple = rdd.flatMap(lambda x: [(y, 1) for y in x[1].split()])
rdd_counts = rdd_tuple.reduceByKey(lambda a, b: a + b)
rdd_joined = rdd_counts.join(rdd3) 

rdd_sorted = rdd_joined.map(lambda x: (x[0], x[1][1], x[1][0])).sortBy(lambda x: x[2], ascending=False).collect()

end_time_rdd = time.time()
print(f"Execution Time (RDDs): {end_time_rdd - start_time_rdd:.2f} seconds")

for item in rdd_sorted:
    print(item)

print("==========================================================")

#Υλοποίηση με Dataframes
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import split, col, explode

crimes_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()), 
    StructField("DATE OCC", StringType()), 
    StructField("TIME OCC", IntegerType()),
    StructField("AREA", IntegerType()),
    StructField("AREA NAME", StringType()),
    StructField("Rpt Dist No", IntegerType()),
    StructField("Part 1-2", IntegerType()),
    StructField("Crm Cd", IntegerType()),
    StructField("Crm Cd Desc", StringType()),
    StructField("Mocodes", StringType()),
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

mocodes_schema = StructType([
    StructField("Column",StringType())
])

crimes_df1 = spark.read.csv("./data/LA_Crime_Data_2010_2019.csv", \
                         header=True, \
                         schema= crimes_schema)

crimes_df1 = crimes_df1.select(col("DR_NO"), col("Mocodes"))

crimes_df2 =  spark.read.csv("./data/LA_Crime_Data_2020_2025.csv", \
                         header=True, \
                         schema= crimes_schema)

crimes_df2 = crimes_df2.select(col("DR_NO"), col("Mocodes"))

mocodes_df = spark.read.csv("./data/MO_codes.txt", \
                         header=False, \
                         schema = mocodes_schema)

mocodes_df = mocodes_df.select(
    split(col("Column"), " ", 2).getItem(0).alias("Mocode"),
    split(col("Column"), " ", 2).getItem(1).alias("Description")
)   

crimes_df = crimes_df1.union(crimes_df2)
crimes_df = crimes_df.select("DR_NO", explode(split(col("Mocodes"), " ")).alias("Mocodes")) #Σπάω το πεδίο των mocodes και κάνω έξτρα σειρές ανάλογα το πλήθος των περιεχομένων (ότι έκανα με flatMap στα rdds)

print(crimes_df.show(10))
print(mocodes_df.show(10))

from pyspark.sql.functions import desc
import time

start_time_df = time.time()

counts_df = crimes_df.groupBy("Mocodes").count()
joined_df = counts_df.join(mocodes_df, counts_df.Mocodes == mocodes_df.Mocode).select("Mocodes","Description","count")
sorted_df = joined_df.orderBy(desc("count"))

print(sorted_df.show())
end_time_df = time.time()
print(f"Execution Time (DataFrame API): {end_time_df - start_time_df:.2f} seconds")

sorted_df.explain(mode="formatted")

print("==========================================================")

# DataFrame with Merge Hint

start_time_df_merge = time.time()

counts_df = crimes_df.groupBy("Mocodes").count()
joined_df_merge = counts_df.hint("merge").join(mocodes_df, counts_df.Mocodes == mocodes_df.Mocode).select("Mocodes","Description","count")
sorted_df = joined_df_merge.orderBy(desc("count"))

print(sorted_df.show())
end_time_df_merge = time.time()
print(f"Execution Time (DataFrame API): {end_time_df_merge - start_time_df_merge:.2f} seconds")

sorted_df.explain(mode="formatted")

print("==========================================================") 
# DataFrame with Shuffle Hash Hint

start_time_df_sh_hash = time.time()

counts_df = crimes_df.groupBy("Mocodes").count()
joined_df_sh_hash = counts_df.hint("shuffle_hash").join(mocodes_df, counts_df.Mocodes == mocodes_df.Mocode).select("Mocodes","Description","count")
sorted_df = joined_df_sh_hash.orderBy(desc("count"))

print(sorted_df.show())
end_time_df_sh_hash = time.time()
print(f"Execution Time (DataFrame API): {end_time_df_sh_hash - start_time_df_sh_hash:.2f} seconds")

sorted_df.explain(mode="formatted")

print("==========================================================")

# DataFrame with Shuffle Replicate NL Hint
start_time_df_sh_rep = time.time()

counts_df = crimes_df.groupBy("Mocodes").count()
joined_df_sh_rep = counts_df.hint("shuffle_replicate_nl").join(mocodes_df, counts_df.Mocodes == mocodes_df.Mocode).select("Mocodes","Description","count")
sorted_df = joined_df_sh_rep.orderBy(desc("count"))

print(sorted_df.show())
end_time_df_sh_rep = time.time()
print(f"Execution Time (DataFrame API): {end_time_df_sh_rep - start_time_df_sh_rep:.2f} seconds")

sorted_df.explain(mode="formatted")

