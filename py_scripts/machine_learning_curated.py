import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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

# Script generated for node S3 bucket accelerometer
S3bucketaccelerometer_node1751486164545 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://em-stedi-lkhouse/accelerometer/trusted/"], "recurse": True}, transformation_ctx="S3bucketaccelerometer_node1751486164545")

# Script generated for node S3 bucket step trainer
S3bucketsteptrainer_node1751486453275 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://em-stedi-lkhouse/step_trainer/trusted/"], "recurse": True}, transformation_ctx="S3bucketsteptrainer_node1751486453275")

# Script generated for node Privacy filter
SqlQuery46 = '''
select * from myDataSource_ac join myDataSource_st on myDataSource_ac.timeStamp = myDataSource_st.sensorReadingTime;
'''
Privacyfilter_node1751486172777 = sparkSqlQuery(glueContext, query = SqlQuery46, mapping = {"myDataSource_ac":S3bucketaccelerometer_node1751486164545, "myDataSource_st":S3bucketsteptrainer_node1751486453275}, transformation_ctx = "Privacyfilter_node1751486172777")

# Script generated for node Drop Fields
DropFields_node1751487467027 = DropFields.apply(frame=Privacyfilter_node1751486172777, paths=["sensorReadingTime"], transformation_ctx="DropFields_node1751487467027")

# Script generated for node Drop Duplicates
DropDuplicates_node1751490801598 =  DynamicFrame.fromDF(DropFields_node1751487467027.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1751490801598")

# Script generated for node Machine Learning Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1751490801598, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751485973335", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
MachineLearningCurated_node1751486177075 = glueContext.write_dynamic_frame.from_options(frame=DropDuplicates_node1751490801598, connection_type="s3", format="json", connection_options={"path": "s3://em-stedi-lkhouse/machine_learning/curated/", "partitionKeys": []}, transformation_ctx="MachineLearningCurated_node1751486177075")

job.commit()