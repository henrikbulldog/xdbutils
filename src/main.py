""" example taken from: 
    https://docs.databricks.com/dev-tools/databricks-connect.html#access-dbutils """

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from xdbutils import fs

spark = SparkSession.builder.getOrCreate()

UNIFORM_PATH = "dbfs:/mnt/dpuniformstoragetest/aad__users/"

print("Contents of ", UNIFORM_PATH)
for line in fs.ls(UNIFORM_PATH):
    print(line)

print("Doppelgangers:")
DF = spark.read.format("delta").load(UNIFORM_PATH) \
    .where(col("displayName").startswith(lit("Henrik"))) \
    .where(col("displayName").endswith(lit("Thomsen"))) \
    .select("displayName", "mail")
DF.show(truncate=False)

print("Rows in big dataset: ",
    spark.read.format("delta") \
        .load("dbfs:/mnt/dpuniformstoragetest/gtms__gtms__gt_alloc_bi").count())
