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

# Script generated for node CustomerTrusted
CustomerTrusted_node1755031583815 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1755031583815")

# Script generated for node Accelerometer
Accelerometer_node1755031540581 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="Accelerometer_node1755031540581")

# Script generated for node SQL Query
SqlQuery6079 = '''
SELECT a.*
FROM accelerometer_landing a
JOIN customer_trusted c
  ON a.user = c.email

'''
SQLQuery_node1755031635362 = sparkSqlQuery(glueContext, query = SqlQuery6079, mapping = {"accelerometer_landing":Accelerometer_node1755031540581, "customer_trusted":CustomerTrusted_node1755031583815}, transformation_ctx = "SQLQuery_node1755031635362")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755031635362, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755030408115", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755031705511 = glueContext.getSink(path="s3://stedi-sri-825/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755031705511")
AmazonS3_node1755031705511.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1755031705511.setFormat("glueparquet", compression="snappy")
AmazonS3_node1755031705511.writeFrame(SQLQuery_node1755031635362)
job.commit()