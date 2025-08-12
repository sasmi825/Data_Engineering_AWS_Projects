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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1755034509559 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1755034509559")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1755034475415 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1755034475415")

# Script generated for node SQL Query
SqlQuery6393 = '''
SELECT
  t.sensorReadingTime,
  t.serialNumber,
  t.distanceFromObject,
  a.user,
  a.x,
  a.y,
  a.z
FROM step_trainer_trusted t
JOIN accelerometer_trusted a
  ON t.sensorReadingTime = a.timestamp;

'''
SQLQuery_node1755034537063 = sparkSqlQuery(glueContext, query = SqlQuery6393, mapping = {"accelerometer_trusted":accelerometer_trusted_node1755034509559, "step_trainer_trusted":step_trainer_trusted_node1755034475415}, transformation_ctx = "SQLQuery_node1755034537063")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755034537063, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755034465369", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755034577809 = glueContext.getSink(path="s3://stedi-sri-825/ml/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755034577809")
AmazonS3_node1755034577809.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1755034577809.setFormat("glueparquet", compression="snappy")
AmazonS3_node1755034577809.writeFrame(SQLQuery_node1755034537063)
job.commit()