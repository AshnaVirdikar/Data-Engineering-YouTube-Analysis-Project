import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame


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
predicate_pushdown = "region in ('ca','gb','us')"  #filtering out and selecting only canadad, germany and us regions data in job
AWSGlueDataCatalog_node1751015506499 = glueContext.create_dynamic_frame.from_catalog(database="dataeng-youtube-raw", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1751015506499", push_down_predicate=predicate_pushdown)

# Script generated for node Change Schema
ChangeSchema_node1751015515371 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1751015506499, mappings=[("video_id", "string", "video_id", "string"), ("trending_date", "string", "trending_date", "string"), ("title", "string", "title", "string"), ("channel_title", "string", "channel_title", "string"), ("category_id", "long", "category_id", "bigint"), ("publish_time", "string", "publish_time", "string"), ("tags", "string", "tags", "string"), ("views", "long", "views", "bigint"), ("likes", "long", "likes", "bigint"), ("dislikes", "long", "dislikes", "bigint"), ("comment_count", "long", "comment_count", "long"), ("thumbnail_link", "string", "thumbnail_link", "string"), ("comments_disabled", "boolean", "comments_disabled", "boolean"), ("ratings_disabled", "boolean", "ratings_disabled", "boolean"), ("video_error_or_removed", "boolean", "video_error_or_removed", "boolean"), ("description", "string", "description", "string"), ("region", "string", "region", "string")], transformation_ctx="ChangeSchema_node1751015515371")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1751015515371, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1751015049936", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})

AmazonS3_node1751015522143 = glueContext.getSink(path="s3://data-engg-on-youtube-cleansed-dev/Youtube/raw_statistics/", connection_type="s3", updateBehavior="LOG", partitionKeys=["region"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1751015522143")
AmazonS3_node1751015522143.setCatalogInfo(catalogDatabase="de_youtube_cleaned",catalogTableName="s3://data-engg-on-youtube-cleansed-dev/Youtube/raw_statistics/")
AmazonS3_node1751015522143.setFormat("glueparquet")
AmazonS3_node1751015522143.writeFrame(ChangeSchema_node1751015515371)
job.commit()
