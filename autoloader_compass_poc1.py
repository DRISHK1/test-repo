# Databricks notebook source
# MAGIC %pip install dlt
# MAGIC %pip install xmltodict

# COMMAND ----------

import dlt, xmltodict, json
xml_path = '/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/TrackS_DMT_230222_230222100859369.xml'
with open(xml_path, 'r', encoding='utf-8') as file:
  xml = file.read()
json_file = xmltodict.parse(xml)
json_file = json.dumps(json_file)
json_file
#dbutils.fs.put('/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/TrackS_DMT_230222_230222100859369.json', contents=json_file, overwrite=True)


# COMMAND ----------

# import json, boto3
# s3 = boto3.resource('s3')
# s3object = s3.Object('gpdipamrasp32394', 'dev/poc_compass/TrackS_DMT_230222_230222100859369.xml')

# s3object.put(Body=(bytes(json_file.encode('UTF-8'))))

df = spark.createDataFrame(json_file)

# COMMAND ----------

@dlt.table
def DMT_TABLE2():
  clusterid = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
  import subprocess
 
  host = "https://pfe-gbi-us-nprod-01.cloud.databricks.com"
  token = "dapi413bf2f8354cca4bd5c2228826f47f07"
  
  pysh = """
  pip install databricks-cli
  rm ~/.databrickscfg
  ~/.databrickscfg
  echo "[DEFAULT]" >> ~/.databrickscfg
  echo "host = {1}" >> ~/.databrickscfg
  echo "token = {2}" >> ~/.databrickscfg
  export DATABRICKS_CONFIG_FILE=~/.databrickscfg

  databricks libraries install --cluster-id {0} --maven-coordinates "com.databricks:spark-xml_2.12:0.14.0"
  databricks libraries list --cluster-id {0}
  """

  subprocess.run(pysh.format(clusterid,host,token),
      shell=True, check=True,
      executable='/bin/bash')
  
  
  
  
  xsd = "/dbfs/mnt/gpdipamrasp32394/dev/poc_compass_xsd/TrackS_DMT_BIX_XML_Schema.xsd"
  spark.sparkContext.addFile(xsd)
  df = spark.read.format("xml").option("rowTag", "PFE-QA-SST-Work-DMT").option("rowValidationXSDPath", "TrackS_DMT_BIX_XML_Schema.xsd").load("s3://gpdipamrasp32394/dev/poc_compass/TrackS_DMT_230222_230222100859369.xml")
  return df
#   return (
#     spark.readStream.format("cloudFiles")
#       .option("cloudFiles.format", "json")
#       .option("inferSchema", "true")
#       .load("/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/")
#   )


# COMMAND ----------

df = spark.read.json("s3://gpdipamrasp32394/dev/poc_compass/TrackS_DMT_230222_230222100859369.json")
# df = spark.readStream.format("cloudFiles")
#     .option("cloudFiles.format", "json")
#     .option("inferSchema", "true")
#     .load("/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/")



# COMMAND ----------

# xsd = "/dbfs/mnt/gpdipamrasp32394/dev/poc_compass_xsd/TrackS_DMT_BIX_XML_Schema.xsd"
# spark.sparkContext.addFile(xsd)
# df = spark.read.format("xml").option("rowTag", "PFE-QA-SST-Work-DMT").option("rowValidationXSDPath", "TrackS_DMT_BIX_XML_Schema.xsd").load("s3://gpdipamrasp32394/dev/poc_compass/TrackS_DMT_230222_230222100859369.xml")
# #df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaEvolutionMode", "addNewColumns").option("cloudFiles.schemaLocation","/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/schema/").option("inferSchema", "true").load("/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/")
# df.write.format("delta").option("checkpointLocation", "/dbfs/mnt/gpdipamrasp32394/dev/poc_compass/schema/checkpoint/").saveAsTable("hive_metastore.compass_schema.dmt_table2")

# COMMAND ----------

# xml = '<?xml version="1.0"?> <catalog> <book id="bk101"> <author>Gambardella, Matthew</author> <title>XML Developers Guide</title> <genre>Computer</genre> <price>44.95</price> <publish_date>2000-10-01</publish_date> <description>An in-depth look at creating applications  with XML.</description> </book> </catalog>'
# json_file = xmltodict.parse(xml)
# dbutils.fs.put('/dbfs/mnt/gpdipamrasp32394/dev/poc_compass2/test.json', contents=str(json_file), overwrite=True)
# df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").option("cloudFiles.schemaEvolutionMode", "addNewColumns").option("cloudFiles.schemaLocation","/dbfs/mnt/gpdipamrasp32394/dev/poc_compass2/schema/").option("inferSchema", "true").load("/dbfs/mnt/gpdipamrasp32394/dev/poc_compass2/")
# df.writeStream.format("delta").option("checkpointLocation", "/dbfs/mnt/gpdipamrasp32394/dev/poc_compass2/schema/checkpoint/").table("hive_metastore.compass_schema.dmt_table_test2")
