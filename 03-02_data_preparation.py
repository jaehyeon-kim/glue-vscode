import sys
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import DataFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

FILE_PATH = "s3://awsglue-datasets/examples/medicare/Medicare_Hospital_Provider.csv"

#### provider id is long except for last 2 records where it is string

## provider id - string <-- considers whole dataset
# medicare_df = (
#     spark.read.format("com.databricks.spark.csv")
#     .option("header", "true")
#     .option("inferSchema", "true")
#     .load(FILE_PATH)
# )

## provider id - string <-- considers whole dataset
# medicare: DynamicFrame = glueContext.create_dynamic_frame.from_options(
#     connection_type="s3",
#     connection_options={"paths": [FILE_PATH]},
#     format="csv",
#     format_options={"withHeader": True},
# )

## provider id - choice -> long / string
## <-- crawler checks only a 2MB prefix of data, lists bigint
medicare: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database="payments", table_name="medicare_hospital_provider_csv"
)

# Where the value was a string that could not be cast, AWS Glue inserted a null.
medicare_res = medicare.resolveChoice(specs=[("provider id", "cast:long")])
# medicare_res.printSchema()

# medicare_res.toDF().where("'provider id' is NULL").count()

medicare_df: DataFrame = medicare_res.toDF().where("'provider id' is NOT NULL")

chop_f = udf(lambda x: x[1:], StringType())
medicare_df = (
    medicare_df.withColumn("ACC", chop_f(medicare_df["average covered charges"]))
    .withColumn("ATP", chop_f(medicare_df["average total payments"]))
    .withColumn("AMP", chop_f(medicare_df["average medicare payments"]))
)

medicare_df.select(["ACC", "ATP", "AMP"]).show()

medicare_tmp_dyf = DynamicFrame.fromDF(medicare_df, glueContext, "nested")
medicare_nest_dyf = medicare_tmp_dyf.apply_mapping(
    [
        ("drg definition", "string", "drg", "string"),
        ("provider id", "long", "provider.id", "long"),
        ("provider name", "string", "provider.name", "string"),
        ("provider city", "string", "provider.city", "string"),
        ("provider state", "string", "provider.state", "string"),
        ("provider zip code", "long", "provider.zip", "long"),
        ("hospital referral region description", "string", "rr", "string"),
        ("ACC", "string", "charges.covered", "double"),
        ("ATP", "string", "charges.total_pay", "double"),
        ("AMP", "string", "charges.medicare_pay", "double"),
    ]
)
medicare_nest_dyf.printSchema()

glueContext.write_dynamic_frame.from_options(
    frame=medicare_nest_dyf,
    connection_type="s3",
    connection_options={"path": "s3://glue-sample-target/output-dir/medicare_parquet"},
    format="parquet",
)
