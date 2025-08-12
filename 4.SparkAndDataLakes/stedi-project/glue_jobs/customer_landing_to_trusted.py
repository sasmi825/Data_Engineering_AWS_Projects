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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755028820274 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1755028820274")

# Script generated for node SQL Query
SqlQuery6085 = '''
select * from customer_landing
where shareWithResearchAsOfDate is not null

'''
SQLQuery_node1755029148832 = sparkSqlQuery(glueContext, query = SqlQuery6085, mapping = {"customer_landing":AWSGlueDataCatalog_node1755028820274}, transformation_ctx = "SQLQuery_node1755029148832")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1755029148832, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1755028670513", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1755029367719 = glueContext.getSink(path="s3://stedi-sri-825/customer/trusted/", connection_type="s3", updateBehavior="LOG", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755029367719")
AmazonS3_node1755029367719.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1755029367719.setFormat("glueparquet", compression="snappy")
AmazonS3_node1755029367719.writeFrame(SQLQuery_node1755029148832)
job.commit()