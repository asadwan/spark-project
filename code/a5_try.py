from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import numpy as np

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

'''4) Do tasks with low priority have a higher probability of being evicted? '''

taskEventsSchema = StructType(
    [
        StructField("timestamp", LongType(), True),
        StructField("missing_info", StringType(), True),
        StructField("job_id", StringType(), True),
        StructField("task_index", StringType(), True),
        StructField("machine_id", StringType(), True),
        StructField("event_type", LongType(), True),
        StructField("user_name", StringType(), True),
        StructField("task_scheduling_class", LongType(), True),
        StructField("priority", LongType(), True),
        StructField("cpu_request", FloatType(), True),
        StructField("memeory_request", FloatType(), True),
        StructField("disk_space_request", FloatType(), True),
        StructField("machine_restrictions", BooleanType(), True),
    ]
)

taskEventsDf = (
    spark.read.schema(taskEventsSchema)
        .csv("../data/task_events_1/*.csv.gz")
        .where(F.col("event_type").isNotNull() & F.col("priority").isNotNull())
)
# Load tasks_events into dataframe and preprocess the data to remove rows that have null values in columns of interest

priority_Event_type_Df = (
    taskEventsDf.select(
        "priority",
        "event_type",

    ).groupBy("priority")
    .agg(
        F.collect_list("event_type").alias("event_types"),
    )
    .sort("priority")

)

Data_Frame_toPandas0 = priority_Event_type_Df.toPandas()


@udf(returnType=FloatType())
def compute_Evicted_event_Per_priority(event_type):
    total_number = len(event_type)
    Evict = event_type.count(2)
    prob = Evict/total_number
    return prob


priority_Event_type_probability_Df = (
    priority_Event_type_Df.withColumn("probability of evicted_event", compute_Evicted_event_Per_priority(F.col("event_types")))

)
priority_event_type = priority_Event_type_probability_Df.drop("event_types")

priority_event_type_toPandas1 = priority_event_type.toPandas()
print(priority_event_type_toPandas1.head())

