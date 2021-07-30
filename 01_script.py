import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import (
    ApplyMapping,
    Filter,
    Map,
    SelectFields,
    Join,
    DropFields,
    SelectFromCollection,
    RenameField,
    Unbox,
    UnnestFrame,
    DropNullFields,
    SplitFields,
    SplitRows,
    Spigot,
)
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import *
from pyspark.sql import Row

glueContext = GlueContext(SparkContext.getOrCreate())

#### Dataset 1

order_list = [
    ["1005", "623", "YES", "1418901234", "75091"],
    ["1006", "547", "NO", "1418901256", "75034"],
    ["1007", "823", "YES", "1418901300", "75023"],
    ["1008", "912", "NO", "1418901400", "82091"],
    ["1009", "321", "YES", "1418902000", "90093"],
]

# Define schema for the order_list
order_schema = StructType(
    [
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("essential_item", StringType()),
        StructField("timestamp", StringType()),
        StructField("zipcode", StringType()),
    ]
)

# Create a Spark Dataframe from the python list and the schema
df_orders = spark.createDataFrame(order_list, schema=order_schema)  # type: ignore


## DynamicFrame
dyf_orders = DynamicFrame.fromDF(df_orders, glueContext, "dyf")

## ApplyMapping
dyf_applyMapping = ApplyMapping.apply(
    frame=dyf_orders,
    mappings=[
        ("order_id", "String", "order_id", "Long"),
        ("customer_id", "String", "customer_id", "Long"),
        ("essential_item", "String", "essential_item", "String"),
        ("timestamp", "String", "timestamp", "Long"),
        ("zipcode", "String", "zip", "Long"),
    ],
)

dyf_applyMapping.printSchema()

## Filter
dyf_filter = Filter.apply(frame=dyf_applyMapping, f=lambda x: x["essential_item"] == "YES")

dyf_filter.toDF().show()

## Map
def next_day_air(rec):
    if rec["zip"] == 75034:
        rec["next_day_air"] = True
    return rec


dyf_map = Map.apply(frame=dyf_applyMapping, f=next_day_air)

dyf_map.toDF().show()

#### Dataset 2

jsonStr1 = u'{ "zip": 75091, "customers": [{ "id": 623, "address": "108 Park Street, TX"}, { "id": 231, "address": "763 Marsh Ln, TX" }]}'
jsonStr2 = u'{ "zip": 82091, "customers": [{ "id": 201, "address": "771 Peek Pkwy, GA" }]}'
jsonStr3 = u'{ "zip": 75023, "customers": [{ "id": 343, "address": "66 P Street, NY" }]}'
jsonStr4 = u'{ "zip": 90093, "customers": [{ "id": 932, "address": "708 Fed Ln, CA"}, { "id": 102, "address": "807 Deccan Dr, CA" }]}'
df_row = spark.createDataFrame(  # type: ignore
    [Row(json=jsonStr1), Row(json=jsonStr2), Row(json=jsonStr3), Row(json=jsonStr4)]
)

df_json = spark.read.json(df_row.rdd.map(lambda r: r.json))  # type: ignore
df_json.show()
df_json.printSchema()

dyf_json = DynamicFrame.fromDF(df_json, glueContext, "dyf_json")

## SelectFields
dyf_selectFields = SelectFields.apply(frame=dyf_filter, paths=["zip"])

dyf_selectFields.toDF().show()

## Join
dyf_join = Join.apply(dyf_json, dyf_selectFields, "zip", "zip")

dyf_join.toDF().show()

## DropFields
dyf_dropFields = DropFields.apply(frame=dyf_join, paths="`.zip`")

dyf_dropFields.toDF().show()

## Relationalize

dyf_relationize = dyf_dropFields.relationalize("root", "/home/glue/GlueLocalOutput")

dyf_relationize.keys()
# dict_keys(["root", "root_customers"])

## SelectFromCollection

dyf_selectFromCollection = SelectFromCollection.apply(dyf_relationize, "root")
dyf_selectFromCollection.toDF().show()

dyf_selectFromCollection = SelectFromCollection.apply(dyf_relationize, "root_customers")
dyf_selectFromCollection.toDF().show()

## RenameField

dyf_renameField_1 = RenameField.apply(
    dyf_selectFromCollection, "`customers.val.address`", "address"
)
dyf_renameField_2 = RenameField.apply(dyf_renameField_1, "`customers.val.id`", "cust_id")

dyf_dropfields_rf = DropFields.apply(frame=dyf_renameField_2, paths=["index", "id"])

dyf_dropfields_rf.toDF().show()

## ResolveChoice

dyf_resolveChoice = dyf_dropfields_rf.resolveChoice(specs=[("cust_id", "cast:String")])

dyf_resolveChoice.printSchema()

#### Dataset 3

warehouse_inventory_list = [
    [
        "TX_WAREHOUSE",
        '{\
                          "strawberry":"220",\
                          "pineapple":"560",\
                          "mango":"350",\
                          "pears":null}',
    ],
    [
        "CA_WAREHOUSE",
        '{\
                         "strawberry":"34",\
                         "pineapple":"123",\
                         "mango":"42",\
                         "pears":null}\
              ',
    ],
    [
        "CO_WAREHOUSE",
        '{\
                         "strawberry":"340",\
                         "pineapple":"180",\
                         "mango":"2",\
                         "pears":null}',
    ],
]


warehouse_schema = StructType(
    [StructField("warehouse_loc", StringType()), StructField("data", StringType())]
)

df_warehouse = spark.createDataFrame(warehouse_inventory_list, schema=warehouse_schema)  # type: ignore
dyf_warehouse = DynamicFrame.fromDF(df_warehouse, glueContext, "dyf_warehouse")

dyf_warehouse.printSchema()

## Unbox

dyf_unbox = Unbox.apply(frame=dyf_warehouse, path="data", format="json")

dyf_unbox.printSchema()
dyf_unbox.toDF().show()

## Unnest

dyf_unnest = UnnestFrame.apply(frame=dyf_unbox)

dyf_unnest.printSchema()

## DropNullFields

dyf_dropNullFields = DropNullFields.apply(frame=dyf_unnest)

dyf_dropNullFields.show()

## SplitFields

dyf_splitFields = SplitFields.apply(
    frame=dyf_dropNullFields, paths=["`data.strawberry`", "`data.pineapple`"], name1="a", name2="b"
)

dyf_splitFields.keys()
# dict_keys(['a', 'b'])

dyf_retrieve_a = SelectFromCollection.apply(dyf_splitFields, "a")
dyf_retrieve_a.show()

dyf_retrieve_b = SelectFromCollection.apply(dyf_splitFields, "b")
dyf_retrieve_b.show()

## SplitRows
dyf_splitRows = SplitRows.apply(
    frame=dyf_dropNullFields,
    comparison_dict={"`data.pineapple`": {">": "100", "<": "200"}},
    name1="pa_200_less",
    name2="pa_200_more",
)

dyf_pa_200_less = SelectFromCollection.apply(dyf_splitRows, "pa_200_less")
dyf_pa_200_less.show()

SelectFromCollection.apply(dyf_splitRows, "pa_200_more").show()

## Spigot
dyf_splitFields = Spigot.apply(
    dyf_dropNullFields, "/home/glue/GlueLocalOutput/Spigot/", {"topk": 1}
)
dyf_splitFields.show()

## Write Dynamic Frame

glueContext.write_dynamic_frame.from_options(
    frame=dyf_splitFields,
    connection_options={"path": "/home/glue/GlueLocalOutput/out/"},
    connection_type="s3",
    format="json",
)
