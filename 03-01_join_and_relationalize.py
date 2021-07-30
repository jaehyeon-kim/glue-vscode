import sys
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Join
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())

DATABASE = "legislators"
OUTPUT_PATH = "s3://glue-python-samples-8aeb31ec/output_dir"
TEMP_PATH = "s3://glue-python-samples-8aeb31ec/temp_dir"

#### examine schemas

persons: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="persons_json"
)

memberships: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="memberships_json"
)

orgs: DynamicFrame = glueContext.create_dynamic_frame.from_catalog(
    database=DATABASE, table_name="organizations_json"
)

#### filter data

orgs = (
    orgs.drop_fields(["other_names", "identifiers"])
    .rename_field("id", "org_id")
    .rename_field("name", "org_name")
)

#### put it all together

l_history: DynamicFrame = Join.apply(
    orgs, Join.apply(persons, memberships, "id", "person_id"), "org_id", "organization_id"
)
l_history = l_history.drop_fields(["person_id", "org_id"])

l_history.printSchema()

# multiple files to support fast parallel reads
glueContext.write_dynamic_frame.from_options(
    frame=l_history,
    connection_type="s3",
    connection_options={"path": f"{OUTPUT_PATH}/legislator_history"},
    format="parquet",
)

# single file
s_history = l_history.toDF().repartition(1)
s_history.write.parquet(f"{OUTPUT_PATH}/legislator_single")

# with partition
l_history.toDF().write.parquet(f"{OUTPUT_PATH}/legislator_part", partitionBy=["org_name"])

#### write to relational databases
dfc = l_history.relationalize("hist_root", f"{TEMP_PATH}/")
dfc.keys()
# dict_keys(
#     [
#         "hist_root",
#         "hist_root_links",
#         "hist_root_images",
#         "hist_root_identifiers",
#         "hist_root_other_names",
#         "hist_root_contact_details",
#     ]
# )

dfc.select("hist_root_contact_details").toDF().where("id = 10 or id = 75").orderBy(
    ["id", "index"]
).show()

dfc.select("hist_root").toDF().where("contact_details = 10 or contact_details = 75").select(
    ["id", "given_name", "family_name", "contact_details"]
).show()

for df_name in dfc.keys():
    m_df = dfc.select(df_name)
    print("Writing to Redshift table: ", df_name)
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=m_df,
        catalog_connection="redshift3",
        connection_options={"dbtable": df_name, "database": "testdb"},
        redshift_tmp_dir="s3://glue-sample-target/temp-dir/",
    )
