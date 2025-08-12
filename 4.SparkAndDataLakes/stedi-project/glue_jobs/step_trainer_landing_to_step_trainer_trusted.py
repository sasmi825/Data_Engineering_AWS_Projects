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

# Script generated for node customer_curated
customer_curated_node1755033954697 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="customer_curated_node1755033954697")

# Script generated for node step_trainer_landing
step_trainer_landing_node1755033931139 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1755033931139")

# Script generated for node SQL Query
SqlQuery6702 = '''
SELECT s.*
FROM step_trainer_landing s
WHERE s.serialNumber IN (
  SELECT DISTINCT serialNumber
  FROM customer_curated
);

'''
SQLQuery_node1755034033566 = sparkSqlQuery(glueContext, query = SqlQuery6702, mapping = {"step_trainer_landing":step_trainer_landing_node1755033931139, "customer_curated":customer_curated_node1755033954697}, transformation_ctx = "SQLQuery_node1755034033566")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755034033566, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755032359246", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755034064986 = glueContext.getSink(path="s3://stedi-sri-825/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755034064986")
AmazonS3_node1755034064986.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1755034064986.setFormat("glueparquet", compression="snappy")
AmazonS3_node1755034064986.writeFrame(SQLQuery_node1755034033566)
job.commit()