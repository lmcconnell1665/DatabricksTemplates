// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC # Scala Notebook to get csv from Azure Blob storage
// MAGIC 
// MAGIC ### Good reference: https://www.sqlshack.com/accessing-azure-blob-storage-from-azure-databricks/

// COMMAND ----------

// connect to blob account
val containerName = "alumni-project-data"
val storageAccountName = "mcconnellsynapsestorage"
val sas = "sp=r&st=2021-03-10T05:10:01Z&se=2021-03-31T12:10:01Z&spr=https&sv=2020-02-10&sr=c&sig=A1x3G6mR0SmsypAKOVdgWyYjSaLfM8PEkzoSg2djJ8M%3D"
val config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"

// COMMAND ----------

// mount the files
dbutils.fs.mount(
  source = "wasbs://alumni-project-data@mcconnellsynapsestorage.blob.core.windows.net/amount#.csv",
  mountPoint = "/mnt/myfile",
  extraConfigs = Map(config -> sas))

// COMMAND ----------

// visualize the data
val mydf = spark.read
.option("header","true")
.option("inferSchema", "true")
.csv("/mnt/myfile")
display(mydf)
