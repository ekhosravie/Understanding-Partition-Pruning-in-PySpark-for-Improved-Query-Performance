# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName('PartitionPurning').getOrCreate()

# COMMAND ----------

data = [(1, "A"), (2, "B"), (3, "A"), (4, "C"), (5, "A")]
df = spark.createDataFrame(data, ["id", "value"]).repartition("value")

# COMMAND ----------

filter_condition = "value = 'A'"
result_with_partition_purning = df.filter(filter_condition)

# COMMAND ----------

print('result with Patition Purning:')
result_with_partition_pruning.show()
print('Execution Plan with Partition Purning:')
result_with_partition_pruning.explain()

# COMMAND ----------

result_without_partition_purning = df.filter(filter_condition)

# COMMAND ----------

print('result plan without partition purning:')
result_without_partition_pruning.show()
print('execution plan without partition purning:')
result_without_partition_pruning.explain()

# COMMAND ----------


