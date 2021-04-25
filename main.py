import pyspark as ps
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructField, StructType, StringType, LongType

spark = SparkSession.builder.appName("Spark Basics").getOrCreate()

schema = StructType(
    [
        StructField(name="city", dataType=StringType(), nullable=True),
        StructField(name="country", dataType=StringType(), nullable=True),
        StructField(name="counts", dataType=LongType(), nullable=False),
    ]
)

rows = [
    Row("Los Angeles", "United States", 3),
    Row("Columbus", "United States", 4),
    Row("London", "United Kingdom", 2),
    Row("Tokyo", "Japan", 3),
]

parallelizeRows = spark.sparkContext.parallelize(rows)
df = spark.createDataFrame(parallelizeRows, schema)
df.show()
