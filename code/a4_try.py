from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

'''4) What can you say about the relation between the scheduling class of a job, the scheduling
class of its tasks, and their priority?'''

jobEventsSchema = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("missing_info", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("job_scheduling_class", LongType(), True),
        StructField("job_name", StringType(), True),
        StructField("logical_job_name", StringType(), True),
    ]
)

taskEventsSchema = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("missing_info", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("task_index", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("user_name", StringType(), True),
        StructField("task_scheduling_class", LongType(), True),
        StructField("priority", LongType(), True),
        StructField("cpu_request", FloatType(), True),
        StructField("memeory_request", FloatType(), True),
        StructField("disk_space_request", FloatType(), True),
        StructField("machine_restrictions", BooleanType(), True),
    ]
)

jobEventsDf = spark.read.schema(jobEventsSchema).csv("../data/job_events/*.csv.gz")
taskEventsDf = spark.read.schema(taskEventsSchema).csv("../data/task_events/*.csv.gz")

job_scheduling_class_Df = jobEventsDf.select("job_id", "job_scheduling_class").where(
    F.col("job_id").isNotNull() & F.col("job_scheduling_class").isNotNull())

task_scheduling_class_and_priority_Df = taskEventsDf.select("job_id", "task_scheduling_class", "priority").where(
    F.col("task_scheduling_class").isNotNull() & F.col("priority").isNotNull() & F.col("job_id").isNotNull())


mean_job_scheduling_class_Per_job_Df = (
    job_scheduling_class_Df
    .groupBy("job_id")
    .agg(
        F.avg("job_scheduling_class").alias("avg_job_scheduling_class"),
    )
)

mean_task_scheduling_class_and_priority_Per_job_Df = (
    task_scheduling_class_and_priority_Df
    .groupBy("job_id")
    .agg(
        F.avg("task_scheduling_class").alias("avg_task_scheduling_class"),
        F.avg("priority").alias("avg_priority"),
    )
)

mean_scheduling_priority_Per_job_Df = mean_task_scheduling_class_and_priority_Per_job_Df.join(
    mean_job_scheduling_class_Per_job_Df, ["job_id"]
)

scheduling_Per_priority_Df = (
    mean_scheduling_priority_Per_job_Df
    .select("avg_priority", "avg_task_scheduling_class", "avg_job_scheduling_class")
    .groupBy("avg_priority")
    .agg(
        F.mean("avg_task_scheduling_class"),
        F.mean("avg_job_scheduling_class")
    )
    .sort("avg_priority")
)

scheduling_Per_priority_Df.coalesce(1).write.csv(
    "../data/results/analysis4/scheduling_Per_priority",
    header=True,
    mode="overwrite",
)


corr_task_scheduling_and_priority_Df = (
    scheduling_Per_priority_Df.
    withColumn("correlation_priority_task_scheduling", F.corr("avg_priority", "avg_task_scheduling_class"))
    .collect()[0]["correlation_priority_task_scheduling"]
)

corr_job_scheduling_and_priority_Df = scheduling_Per_priority_Df.withColumn("correlation_priority_job_scheduling", F.corr("avg_priority", "avg_job_scheduling_class")).collect()[0]["correlation_priority_job_scheduling"]
corr_task_scheduling_and_priority_Df = scheduling_Per_priority_Df.withColumn("correlation_priority_task_scheduling", F.corr("avg_priority", "avg_task_scheduling_class")).collect()[0]["correlation_priority_task_scheduling"]
# Pandas__Data_Frame_0 = scheduling_class_priority_task_event_Df.toPandas()

print('corr_task_scheduling_and_priority:', corr_task_scheduling_and_priority_Df)
print('corr_job_scheduling_and_priority:', corr_job_scheduling_and_priority_Df)
