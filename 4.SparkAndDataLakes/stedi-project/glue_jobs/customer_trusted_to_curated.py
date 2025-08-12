import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer_trusted
Customer_trusted_node1755032398989 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Customer_trusted_node1755032398989")

# Script generated for node Accelerometer_trusted
Accelerometer_trusted_node1755032383089 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="Accelerometer_trusted_node1755032383089")

# Script generated for node SQL Query
SqlQuery6278 = '''
SELECT DISTINCT c.*
FROM customer_trusted c
JOIN accelerometer_trusted a
  ON c.email = a.email;

'''
SQLQuery_node1755032432708 = sparkSqlQuery(glueContext, query = SqlQuery6278, mapping = {"accelerometer_trusted":Accelerometer_trusted_node1755032383089, "customer_trusted":Customer_trusted_node1755032398989}, transformation_ctx = "SQLQuery_node1755032432708")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755032432708, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755032359246", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755032605233 = glueContext.getSink(path="s3://stedi-sri-825/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755032605233")
AmazonS3_node1755032605233.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1755032605233.setFormat("glueparquet", compression="snappy")
AmazonS3_node1755032605233.writeFrame(SQLQuery_node1755032432708)
job.commit()