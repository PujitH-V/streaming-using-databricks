# Databricks notebook source
# MAGIC %scala
# MAGIC
# MAGIC Class.forName("org.apache.spark.sql.eventhubs.EventHubsSource")

# COMMAND ----------

# DBTITLE 1,Method 1
namespaceConnectionString = "Endpoint=sb://saevhub.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=4XOv2oonFW+7qyrkPaMsGEAOxyMYsYNiP+AEhCwAstk="
# Endpoint=sb://saevhub.servicebus.windows.net/;SharedAccessKeyName=Databricks_SAS_Policy;SharedAccessKey=5XXZ+1VQcaNdpMnPHYirxJoWQ+H90N4Pc+AEhB4IoE4=;EntityPath=testhub

eventHubName = "testhub"

eventHubConnectionString = f"{namespaceConnectionString};EntityPath={eventHubName}"

# eventHubConnectionString = "Endpoint=sb://saevhub.servicebus.windows.net/;SharedAccessKeyName=Databricks_SAS_Policy;SharedAccessKey=5XXZ+1VQcaNdpMnPHYirxJoWQ+H90N4Pc+AEhB4IoE4=;EntityPath=testhub"

eventHubConfiguration = {'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(eventHubConnectionString)}

df_method1 = spark.readStream.format('eventhubs').options(**eventHubConfiguration).load()

# COMMAND ----------

df_method1.writeStream.queryName("MemoryQuery").format("memory").trigger(processingTime='2 seconds').start()

# COMMAND ----------

display(
    df_method1,
    streamName = "DisplayMemoryQuery",
    processingTime = '10 seconds'
)

# COMMAND ----------

df_method1.lastProgress

# COMMAND ----------

df_method1.isStreaming

# COMMAND ----------

df_method1.display()

# COMMAND ----------

df_method1.lastProgress

# COMMAND ----------

# DBTITLE 1,Method 2
eventHubsConf = {
  "eventhubs.namespace": "testhub",
  "eventhubs.name": "saevhub",
  "eventhubs.sas.key.name": "RootManageSharedAccessKey",
  "eventhubs.sas.key": "4XOv2oonFW+7qyrkPaMsGEAOxyMYsYNiP+AEhCwAstk=",
  "eventhubs.consumer.group": "$Default",
  "spark.eventhubs.startingPosition": "{ \"startOfStream\":true }"
}

df_method1 = spark.readStream.format("eventhubs").options(**eventHubsConf).load()
