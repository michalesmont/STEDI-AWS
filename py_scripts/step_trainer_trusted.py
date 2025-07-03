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

# Script generated for node S3 bucket step trainer
S3bucketsteptrainer_node1751486164545 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://em-stedi-lkhouse/step_trainer/landing/"], "recurse": True}, transformation_ctx="S3bucketsteptrainer_node1751486164545")

# Script generated for node S3 bucket customer
S3bucketcustomer_node1751486453275 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://em-stedi-lkhouse/customer/curated/"], "recurse": True}, transformation_ctx="S3bucketcustomer_node1751486453275")

# Script generated for node Privacy filter
SqlQuery14 = '''
select * from myDataSource_tr join myDataSource_cs on myDataSource_tr.serialNumber = myDataSource_cs.serialNumber;
'''
Privacyfilter_node1751486172777 = sparkSqlQuery(glueContext, query = SqlQuery14, mapping = {"myDataSource_tr":S3bucketsteptrainer_node1751486164545, "myDataSource_cs":S3bucketcustomer_node1751486453275}, transformation_ctx = "Privacyfilter_node1751486172777")

# Script generated for node Drop Fields
DropFields_node1751487467027 = DropFields.apply(frame=Privacyfilter_node1751486172777, paths=["customerName", "email", "phone", "birthDay", "registrationDate", "lastUpdateDate", "shareWithResearchAsOfDate", "shareWithPublicAsOfDate", "shareWithFriendsAsOfDate"], transformation_ctx="DropFields_node1751487467027")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=DropFields_node1751487467027, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751485973335", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1751486177075 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1751487467027, connection_type="s3", format="json", connection_options={"path": "s3://em-stedi-lkhouse/step_trainer/trusted/", "partitionKeys": []}, transformation_ctx="StepTrainerTrusted_node1751486177075")

job.commit()