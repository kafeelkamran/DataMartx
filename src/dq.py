import os
os.environ["SPARK_VERSION"] = "3.3"  # Match your PySpark version

from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DataQualityChecks").getOrCreate()

import os

base_dir = os.path.dirname(__file__)
csv_path = os.path.join(base_dir, "data", "sales.csv")

data = spark.read.csv(csv_path, header=True, inferSchema=True)
#dsfsdfdsfdfsdf
# Load sample data
data = spark.read.csv("src\data\sales.csv",header=True,inferSchema=True)

# Define quality rules
check = Check(spark, CheckLevel.Error, "Data quality checks") \
        .hasSize(lambda x: x >= 10000) \
        .isComplete("product_id") \
        .isNonNegative("actual_price")

# Run verification
result = VerificationSuite(spark) \
        .onData(data) \
        .addCheck(check) \
        .run()

if result.status != "Success":
    print("Data quality checks failed. Blocking deployment.")
    exit(1)
