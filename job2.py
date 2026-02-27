import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# 1. Lecture des fichiers
application = glueContext.create_dynamic_frame.from_options(
    format_options={}, connection_type="s3", format="parquet",
    connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/processed/"], "recurse": True}
).toDF()

previous_app = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3", format="csv",
    connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/previous_application-LFS.txt"], "recurse": True}
).toDF()

installments = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3", format="csv",
    connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/installments_payments-LFS.txt"], "recurse": True}
).toDF()

pos_cash = glueContext.create_dynamic_frame.from_options(
    format_options={"quoteChar": "\"", "withHeader": True, "separator": ","},
    connection_type="s3", format="csv",
    connection_options={"paths": ["s3://projet-big-data-credit-beloin-lucas/raw/POS_CASH_balance-LFS.txt"], "recurse": True}
).toDF()

# 2. Cast des colonnes numériques
installments = installments.withColumn("days_entry_payment", F.col("days_entry_payment").cast("double"))
previous_app = previous_app.withColumn("amt_application", F.col("amt_application").cast("double")) \
                            .withColumn("days_decision", F.col("days_decision").cast("int"))

# 3. Aggregate installments par sk_id_prev
installments_agg = installments.groupBy("sk_id_prev").agg(
    F.max("days_entry_payment").alias("max_days_entry_payment"),
    F.avg("days_entry_payment").alias("avg_days_entry_payment")
)

# 4. Aggregate POS_CASH par sk_id_prev
pos_cash_agg = pos_cash.groupBy("sk_id_prev").agg(
    F.count("months_balance").alias("count_months_balance")
)

# 5. Join previous_app + installments_agg
merged = previous_app.join(installments_agg, on="sk_id_prev", how="left")

# 6. Join + pos_cash_agg
merged = merged.join(pos_cash_agg, on="sk_id_prev", how="left")

# 7. Aggregate final par sk_id_curr
final_agg = merged.groupBy("sk_id_curr").agg(
    F.count("sk_id_prev").alias("count_prev_applications"),
    F.sum("amt_application").alias("sum_amt_application"),
    F.max("days_decision").alias("max_days_decision"),
    F.max("max_days_entry_payment").alias("max_days_entry_payment"),
    F.avg("avg_days_entry_payment").alias("avg_days_entry_payment"),
    F.sum("count_months_balance").alias("sum_count_months_balance")
)

# 8. Join final avec application
final = application.join(final_agg, on="sk_id_curr", how="left")

# 9. Ecriture S3
from awsglue.dynamicframe import DynamicFrame
final_dyf = DynamicFrame.fromDF(final, glueContext, "final")

glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://projet-big-data-credit-beloin-lucas/Sortie Job2/",
        "partitionKeys": []
    },
    format_options={"compression": "snappy"}
)

job.commit()