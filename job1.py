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

# Script generated for node bureau_balance-LFS
bureau_balanceLFS_node1771440167604 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/bureau_balance-LFS.txt"], "recurse": True}, transformation_ctx="bureau_balanceLFS_node1771440167604")

# Script generated for node application_train-LFS
application_trainLFS_node1771444625546 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/application_train-LFS.txt"], "recurse": True}, transformation_ctx="application_trainLFS_node1771444625546")

# Script generated for node bureau-LFS
bureauLFS_node1771440166485 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/bureau-LFS.txt"], "recurse": True}, transformation_ctx="bureauLFS_node1771440166485")

# Script generated for node Aggregate
Aggregate_node1771442311158 = sparkAggregate(glueContext, parentFrame = bureau_balanceLFS_node1771440167604, groups = ["sk_id_bureau"], aggs = [["months_balance", "min"], ["months_balance", "max"]], transformation_ctx = "Aggregate_node1771442311158")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1771444460980 = ApplyMapping.apply(frame=bureauLFS_node1771440166485, mappings=[("sk_id_curr", "string", "sk_id_curr", "string"), ("sk_id_bureau", "string", "right_sk_id_bureau", "string"), ("credit_active", "string", "credit_active", "string"), ("credit_currency", "string", "credit_currency", "string"), ("days_credit", "string", "days_credit", "int"), ("credit_day_overdue", "string", "credit_day_overdue", "int"), ("days_credit_enddate", "string", "days_credit_enddate", "string"), ("days_enddate_fact", "string", "days_enddate_fact", "string"), ("amt_credit_max_overdue", "string", "amt_credit_max_overdue", "string"), ("cnt_credit_prolong", "string", "cnt_credit_prolong", "string"), ("amt_credit_sum", "string", "amt_credit_sum", "double"), ("amt_credit_sum_debt", "string", "amt_credit_sum_debt", "string"), ("amt_credit_sum_limit", "string", "amt_credit_sum_limit", "string"), ("amt_credit_sum_overdue", "string", "amt_credit_sum_overdue", "string"), ("credit_type", "string", "credit_type", "string"), ("days_credit_update", "string", "days_credit_update", "string"), ("amt_annuity", "string", "amt_annuity", "string")], transformation_ctx="RenamedkeysforJoin_node1771444460980")

# Script generated for node Join
RenamedkeysforJoin_node1771444460980DF = RenamedkeysforJoin_node1771444460980.toDF()
Aggregate_node1771442311158DF = Aggregate_node1771442311158.toDF()
Join_node1771443972463 = DynamicFrame.fromDF(RenamedkeysforJoin_node1771444460980DF.join(Aggregate_node1771442311158DF, (RenamedkeysforJoin_node1771444460980DF['right_sk_id_bureau'] == Aggregate_node1771442311158DF['sk_id_bureau']), "left"), glueContext, "Join_node1771443972463")

# Script generated for node Aggregate_2
Aggregate_2_node1771442816164 = sparkAggregate(glueContext, parentFrame = Join_node1771443972463, groups = ["sk_id_curr"], aggs = [["amt_credit_sum", "avg"], ["amt_credit_sum", "max"], ["amt_credit_sum", "sum"], ["days_credit", "min"], ["days_credit", "max"], ["credit_day_overdue", "max"], ["`min(months_balance)`", "min"], ["`max(months_balance)`", "max"]], transformation_ctx = "Aggregate_2_node1771442816164")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1772100469605 = ApplyMapping.apply(frame=Aggregate_2_node1771442816164, mappings=[("sk_id_curr", "string", "right_sk_id_curr", "string"), ("`avg(amt_credit_sum)`", "double", "`right_avg(amt_credit_sum)`", "double"), ("`max(amt_credit_sum)`", "double", "`right_max(amt_credit_sum)`", "double"), ("`sum(amt_credit_sum)`", "double", "`right_sum(amt_credit_sum)`", "double"), ("`min(days_credit)`", "int", "`right_min(days_credit)`", "int"), ("`max(days_credit)`", "int", "`right_max(days_credit)`", "int"), ("`max(credit_day_overdue)`", "int", "`right_max(credit_day_overdue)`", "int"), ("`min(min(months_balance))`", "string", "right_min_months_balance", "int"), ("`max(max(months_balance))`", "string", "right_max_months_balance", "int")], transformation_ctx="RenamedkeysforJoin_node1772100469605")

# Script generated for node Join_final
application_trainLFS_node1771444625546DF = application_trainLFS_node1771444625546.toDF()
RenamedkeysforJoin_node1772100469605DF = RenamedkeysforJoin_node1772100469605.toDF()
Join_final_node1772099645096 = DynamicFrame.fromDF(application_trainLFS_node1771444625546DF.join(RenamedkeysforJoin_node1772100469605DF, (application_trainLFS_node1771444625546DF['sk_id_curr'] == RenamedkeysforJoin_node1772100469605DF['right_sk_id_curr']), "left"), glueContext, "Join_final_node1772099645096")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_final_node1772099645096, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1772096880009", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1772101437788 = glueContext.write_dynamic_frame.from_options(frame=Join_final_node1772099645096, connection_type="s3", format="glueparquet", connection_options={"path": "s3://projet-big-data-credit-beloin-lucas/processed/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1772101437788")

job.commit()