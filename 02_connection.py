import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

## read from S3
ref_data = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": ["s3://analytics-workshop-20210721/data/reference_data/tracks_list.json"]
    },
    format="json",
)

ref_data.show()

## read from catalog
ref_data1 = glueContext.create_dynamic_frame.from_catalog(
    database="analyticsworkshopdb", table_name="reference_data", transformation_ctx="ref_data"
)

ref_data1.show()
