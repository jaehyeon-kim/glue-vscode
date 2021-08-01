from awsglue.dynamicframe import DynamicFrame

def filter_dynamic_frame(dyf: DynamicFrame, column_name: str = "age", value: int = 20):
    return dyf.filter(f=lambda x: x[column_name] > value)
