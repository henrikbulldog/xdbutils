""" example taken from: 
    https://docs.databricks.com/dev-tools/databricks-connect.html#access-dbutils """

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode
from xdbutils import XDBUtils

spark = SparkSession.builder.getOrCreate()
xdbutils = XDBUtils()

spark.conf.set("fs.s3n.awsAccessKeyId", os.getenv("AWS_ACCESS_KEY_ID", None))
spark.conf.set("fs.s3n.awsSecretAccessKey", os.getenv("AWS_SECRET_ACCESS_KEY", None))

bucket_name = os.getenv("AWS_S3_BUCKET", "My-bucket")
folder = "bronze/eds"
file_name = "co2emis.json"

s3_path = f"s3://{bucket_name}/{folder}"
print("Contents of ", s3_path)
for line in xdbutils.fs.ls(s3_path):
    print(line)

df = spark.read \
    .option("multiline", True) \
    .json(s3_path) \
    .withColumn("record", explode("records")) \
    .select("record.CO2Emission",
        col("record.Minutes5DK").cast("timestamp").alias("Minutes5DK"),
        "record.PriceArea")

df.show(truncate=False)
