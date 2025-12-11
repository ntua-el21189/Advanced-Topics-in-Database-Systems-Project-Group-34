from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,TimestampNTZType,StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .appName("Q2 DF implementation") \
    .getOrCreate()

# Define the schema for the Crimes Data Frame 

crimes_schema = StructType([
    StructField("DR_NO", IntegerType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE_OCC", StringType()),
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

crimes_df1 = spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/LA_Crime_Data/LA_Crime_Data_2010_2019.csv", \
                         header=True, \
                         schema= crimes_schema)

crimes_df2 =  spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/LA_Crime_Data/LA_Crime_Data_2020_2025.csv", \
                         header=True, \
                         schema= crimes_schema)
REcodes_schema = StructType([
    StructField("Vict_Descent", StringType()),
    StructField("Vict_Descent_Full", StringType())
])
REcodes_df =spark.read.csv("s3://initial-notebook-data-bucket-dblab-905418150721/project_data/RE_codes.csv", \
                         header=True, \
                         schema= REcodes_schema)
REcodes_df.printSchema()

crimes_df1 = crimes_df1.select(col("DATE_OCC"), col("Vict_Descent"))
crimes_df2 = crimes_df2.select(col("DATE_OCC"), col("Vict_Descent"))
crimes_df = crimes_df1.union(crimes_df2)
crimes_df.printSchema()
crimes_df.show(2) # για να υπολογιστεί το crimes και να μην συνυπολογιστεί στο χρόνο του Dataframe

# μετατρέπω τη στήλη DATA OCC σε timestamp
crimes_df_final = crimes_df.withColumn("DATE_OCC", sf.year(to_timestamp("DATE_OCC", 
                "yyyy MMM dd hh:mm:ss a")))
crimes_df_final = crimes_df_final.withColumnRenamed("DATE_OCC", "Year")
crimes_df_final.printSchema()

# start time
import time
start_time_df = time.time()
crimes_df_grouped = (
    crimes_df_final.groupBy("Year", "Vict_Descent")
                   .count().sort(sf.desc("Year"))
)
rank_window_spec = Window.partitionBy("Year").orderBy(sf.desc("count"))
total_window_spec = Window.partitionBy("Year")
q1_result = crimes_df_grouped.withColumn(
    "total_per_year",
    sf.sum("count").over(total_window_spec)

).withColumn(
    "%",
    sf.format_number( (sf.col("count") / sf.col("total_per_year")) * 100, 2 )

).withColumn(
    "rank",
    sf.row_number().over(rank_window_spec)
).where(
    sf.col("rank") <= 3

)
q1_result.join(REcodes_df, "Vict_Descent")\
    .select("Year", "Vict_Descent_Full", col("count").alias("Number_of_Victims"),"%").sort(sf.desc("Year")).show()
end_time_df = time.time()
print(f"Execution Time (DataFrame API): {end_time_df - start_time_df:.2f} seconds")
#end time

crimes_df.createOrReplaceTempView("crimes1")
REcodes_df.createOrReplaceTempView("REcodes")
prep_crimes="SELECT Vict_Descent FROM crimes1 LIMIT (2)"
prep_re="SELECT Vict_Descent  FROM REcodes LIMIT (2)"
prep_crimes=spark.sql(prep_crimes)
prep_crimes.show()
prep_re=spark.sql(prep_re)
prep_re.show()

sql_helper = "SELECT YEAR(TO_TIMESTAMP( DATE_OCC , 'yyyy MMM dd hh:mm:ss a')) AS Year, Vict_Descent FROM crimes1"
crimes_final=spark.sql(sql_helper)
crimes_final.createOrReplaceTempView("crimes")

# start time
import time
start_time_sql = time.time()
sql_query = """
WITH 
GroupedCounts AS (
    SELECT
        Year,  
        Vict_Descent,
        COUNT(*) AS count
    FROM crimes
    GROUP BY
        Year,  
        Vict_Descent
),

WindowCalculations AS (
    SELECT
        Year,
        Vict_Descent,
        count,
        SUM(count) OVER (PARTITION BY Year) AS total_per_year,
        ROW_NUMBER() OVER (PARTITION BY Year ORDER BY count DESC) AS rank   
    FROM GroupedCounts
),
RankedAndPercent AS (
    SELECT
        Year,
        Vict_Descent,
        count,
        rank,
        FORMAT_NUMBER((count / total_per_year) * 100.0, 2) AS percent
    FROM WindowCalculations
    WHERE rank <= 3
)
SELECT
    Year,
    REcodes.Vict_Descent_Full,
    count AS Number_of_Victims,
    percent
FROM RankedAndPercent 
JOIN REcodes 
    ON RankedAndPercent.Vict_Descent = REcodes.Vict_Descent
ORDER BY
    RankedAndPercent.Year DESC  
"""

q1_result_from_sql = spark.sql(sql_query)
q1_result_from_sql.show()
end_time_sql = time.time()
print(f"Execution Time (SQL API): {end_time_sql - start_time_sql:.2f} seconds")
#end time
