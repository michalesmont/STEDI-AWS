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

# Script generated for node S3 bucket
S3bucket_node1751322418946 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://em-stedi-lkhouse/customer/landing/"], "recurse": True}, transformation_ctx="S3bucket_node1751322418946")

# Script generated for node Privacy Filter
SqlQuery1929 = '''
select * from myDataSource where sharewithresearchasofdate <> 0;
'''
PrivacyFilter_node1751322430377 = sparkSqlQuery(glueContext, query = SqlQuery1929, mapping = {"myDataSource":S3bucket_node1751322418946}, transformation_ctx = "PrivacyFilter_node1751322430377")

# Script generated for node Trusted Consumer Zone
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1751322430377, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751321036139", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
TrustedConsumerZone_node1751322434812 = glueContext.getSink(path="s3://em-stedi-lkhouse/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="TrustedConsumerZone_node1751322434812")
TrustedConsumerZone_node1751322434812.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
TrustedConsumerZone_node1751322434812.setFormat("json")
TrustedConsumerZone_node1751322434812.writeFrame(PrivacyFilter_node1751322430377)
job.commit()