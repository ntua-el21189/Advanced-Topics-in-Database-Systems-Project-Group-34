%%configure -f
{
    "conf":{
        "spark.executor.instances": "4",
        "spark.executor.memory": "2g",
        "spark.executor.cores": "1"
    }
}
#commit test
#Υλοποίηση με RDDs
from pyspark.sql import SparkSession
import csv
import time

sc = spark.sparkContext

rdd1 = sc.textFile("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv")\
    .map(lambda line: next(csv.reader([line])))\
    .map(lambda x: [x[9], x[11]]) 

rdd2 = sc.textFile("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv")\
    .map(lambda line: next(csv.reader([line])))\
    .map(lambda x: [x[9], x[11]]) 

rdd = rdd1.union(rdd2)
header = rdd.first()
rdd = rdd.filter(lambda x: x!=header)
rdd = rdd.filter(lambda x: "AGGRAVATED ASSAULT" in x[0])

# Aντιστοιχίζω κάθε εγγραφή στην ηλικιακή ομάδα της
def assign_age_group(age_str):
    age = int(age_str)
    if age < 18:
        return "0-17 Children"
    elif age >= 18 and age <= 24:
        return "18-24 Young Adults"
    elif age >= 25 and age <= 64:
        return "25-64 Adults"
    else:
        return "65+ Elderly"
        
start_time_rdd = time.time()

# Νέο RDD με το όνομα της ηλικιακής ομάδας
rdd_tuples = rdd.map(lambda x: (assign_age_group(x[1]),1))

# Καταμέτρηση των εμφανίσεων κάθε ομάδας
rdd_counts = rdd_tuples.reduceByKey(lambda a, b: a + b)

#Sort
rdd_sorted = rdd_counts.sortBy(lambda x: x[1], ascending=False)

print(rdd_sorted.collect())

end_time_rdd = time.time()
print(f"Execution Time (RDDs): {end_time_rdd - start_time_rdd:.2f} seconds")

#Υλοποίηση με DataFrames
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col

crimes_schema = StructType([
    StructField("DR_NO", IntegerType()),
    StructField("Date Rptd", StringType()), #Να το αλλάξω
    StructField("DATE OCC", StringType()), #Να το αλλάξω
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

crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv", \
                         header=True, \
                         schema= crimes_schema)

crimes_df1 = crimes_df1.select(col("Crm Cd Desc"), col("Vict Age"))

crimes_df2 =  spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv", \
                         header=True, \
                         schema= crimes_schema)

crimes_df2 = crimes_df2.select(col("Crm Cd Desc"), col("Vict Age"))
                               
crimes_df = crimes_df1.union(crimes_df2)
crimes_df = crimes_df.filter(col("Crm Cd Desc").contains("AGGRAVATED ASSAULT"))
print(crimes_df.show(10))

from pyspark.sql.functions import when, desc
import time
start_time_df = time.time()

df_categorized = crimes_df.withColumn(
    "Age_Group",
    when(col("Vict Age") < 18, "0-17 Children")
     .when((col("Vict Age") >= 18) & (col("Vict Age") <= 24), "18-24 Young Adults")
     .when((col("Vict Age") >= 25) & (col("Vict Age") <= 64), "25-64 Adults")
     .otherwise("65+ Elderly")
)

# GroupBy την Age_Group
df_counts = df_categorized.groupBy("Age_Group").count()

# Ταξινόμηση κατά φθίνουσα σειρά με βάση τη στήλη count
df_sorted = df_counts.orderBy(desc("count"))

print(df_sorted.show())
end_time_df = time.time()
print(f"Execution Time (DataFrame API): {end_time_df - start_time_df:.2f} seconds")

#Με UDFs
from pyspark.sql.functions import udf

start_time_udf = time.time()

def assign_age_group(age_str):
    age = int(age_str)
    if age < 18:
        return "0-17 Children"
    elif age >= 18 and age <= 24:
        return "18-24 Young Adults"
    elif age >= 25 and age <= 64:
        return "25-64 Adults"
    else:
        return "65+ Elderly"

assign_age_group_udf = udf(assign_age_group, StringType())

# Εφαρμογή της UDF στη στήλη
df_categorized = crimes_df.withColumn("Age_Group",assign_age_group_udf(col("Vict Age")))

# GroupBy την Age_Group
df_counts = df_categorized.groupBy("Age_Group").count()

# Ταξινόμηση κατά φθίνουσα σειρά με βάση τη στήλη count
df_sorted = df_counts.orderBy(desc("count"))

print(df_sorted.show())
end_time_udf = time.time()
print(f"Execution Time (UDFs): {end_time_udf - start_time_udf:.2f} seconds")
