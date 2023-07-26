# Databricks notebook source
hubs = ['orders-hub','customers-hub','products-hub','Vendor-Store-Hub']

# COMMAND ----------

for i in hubs:
    namespaceConnectionString = 'Endpoint=sb://saevhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4XOv2oonFW+7qyrkPaMsGEAOxyMYsYNiP+AEhCwAstk='
    eventHubName = i
    eventHubConnectionString = f'{namespaceConnectionString};EntityPath={eventHubName}'
    eventHubConfiguration = {'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString)}
    exec(f"""df_{i.replace('-','_')} = spark.readStream.format('eventhubs').options(**eventHubConfiguration).load()""")

# COMMAND ----------

StreamingMemoryQuery_Customers = df_customers_hub.writeStream.queryName("MemoryQuery").format("memory").trigger(processingTime= '10 seconds').start()

# COMMAND ----------

StreamingMemoryQuery_Orders.lastProgress

# COMMAND ----------

display(
    df_orders_hub,
    streamName = "DisplayMemoryQuery",
    processingTime = '10 seconds'
)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

staging_orders = (
    df_orders_hub.withColumn("RawData",col("body").cast("string")).withColumn('LoadDateTime',current_timestamp())
)

# COMMAND ----------

Bronze_orders = staging_orders.select(staging_orders.columns[1:])

# COMMAND ----------

Bronze_orders.printSchema()

# COMMAND ----------


