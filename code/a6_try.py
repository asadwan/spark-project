from pyspark import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import numpy as np
import matplotlib.pyplot as plt

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

spark = SparkSession(sc)

'''6) In general, do tasks from the same job run on the same machine? '''

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
        .csv("../data/task_events/*.csv.gz")
        .where(F.col("machine_id").isNotNull() & F.col("job_id").isNotNull())
)
# Load tasks_events into dataframe and preprocess the data to remove rows that have null values in columns of interest

machine_job_Df = taskEventsDf.select("machine_id", "job_id").distinct().groupBy("job_id").count().withColumnRenamed("count", "No.different machine_ids for each job_id")
DataFrame_To_pandas_0 = machine_job_Df.toPandas()
DataFrame_To_pandas_0.plot(x= "job_id", y = "No.different machine_ids for each job_id")

frame = plt.gca()
# hide x-axis
frame.axes.get_xaxis().set_visible(False)
plt.show()
print('')

