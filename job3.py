import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
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

# Script generated for node JOB2
JOB2_node1772111644980 = glueContext.create_dynamic_frame.from_options(format_options={}, connection_type="s3", format="parquet", connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/Sortie Job2/"], "recurse": True}, transformation_ctx="JOB2_node1772111644980")

# Script generated for node credit_card_balance-LFS
credit_card_balanceLFS_node1771405478336 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/credit_card_balance-LFS.txt"], "recurse": True}, transformation_ctx="credit_card_balanceLFS_node1771405478336")

# Script generated for node Aggregate
Aggregate_node1772111677619 = sparkAggregate(glueContext, parentFrame = credit_card_balanceLFS_node1771405478336, groups = ["sk_id_curr"], aggs = [["amt_balance", "avg"], ["amt_credit_limit_actual", "max"]], transformation_ctx = "Aggregate_node1772111677619")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1772111998257 = ApplyMapping.apply(frame=Aggregate_node1772111677619, mappings=[("sk_id_curr", "string", "right_sk_id_curr", "string"), ("`avg(amt_balance)`", "double", "`right_avg(amt_balance)`", "double"), ("`max(amt_credit_limit_actual)`", "string", "`right_max(amt_credit_limit_actual)`", "double")], transformation_ctx="RenamedkeysforJoin_node1772111998257")

# Script generated for node Join
JOB2_node1772111644980DF = JOB2_node1772111644980.toDF()
RenamedkeysforJoin_node1772111998257DF = RenamedkeysforJoin_node1772111998257.toDF()
Join_node1772111723691 = DynamicFrame.fromDF(JOB2_node1772111644980DF.join(RenamedkeysforJoin_node1772111998257DF, (JOB2_node1772111644980DF['sk_id_curr'] == RenamedkeysforJoin_node1772111998257DF['right_sk_id_curr']), "left"), glueContext, "Join_node1772111723691")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1772111723691, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772111615022", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1772111784780 = glueContext.write_dynamic_frame.from_options(frame=Join_node1772111723691, connection_type="s3", format="glueparquet", connection_options={"path": "s3://projet-big-data-credit-beloin-lucas/Sortie Job3/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1772111784780")

job.commit()