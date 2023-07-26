-- Databricks notebook source
create table if not exists bronze_orders_hub
(
    partition String,
    offset String,
    sequenceNumber long,
    enqueuedTime timestamp,
    publisher String,
    partitionKey String,
    properties String,
    systemProperties String,
    RawData String,
    LoadDateTime timestamp
)
USING DELTA
LOCATION '/mnt/warehouse/Bronze_Tables/Orders_Hub/'

-- COMMAND ----------


