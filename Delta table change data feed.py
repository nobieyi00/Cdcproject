# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC Prerequiste
# MAGIC 1. Create External table on delta file
# MAGIC 2. Enable Change Data feed for delta table
# MAGIC 
# MAGIC Scenario: Existing Pipeline is running slow when transfering data from raw to refine storage container. We want to optimize it from using Lastmodifieddate filtering of files to use change data feed feature

# COMMAND ----------

spark.sql("CREATE  TABLE Employee_raw USING DELTA LOCATION '/mnt/raw/Employee'")
spark.sql("ALTER TABLE Employee_raw SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

from pyspark.sql.functions import lit
from delta.tables import *
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md 
# MAGIC Full load

# COMMAND ----------

#Read all data in input landing zone
Employee_df = spark.read.option("delimiter", ",").option("header", "true").csv("/mnt/landing/input/Employee")

Employee_df.createOrReplaceTempView("Employee_tbl")

#By default databricks will merge schema so we merge both full and incr files schema and get the latest version of each business key record based on modifieddate
Employee_Source_df = spark.sql("""
select * 
from (
        select *, row_number () over (partition by ID order by Modifieddate desc) as Rn 
        from (
                select ID, Name, Weight, coalesce(Modifieddate, '1900-01-01 12:00:00') as Modifieddate, coalesce (TransactionType, 'INSERT') as TransactionType
                from Employee_tbl
             ) sub1
    ) sub2
where sub2.rn =1
""")
#drop Rownumber field
Employee_Source_df = Employee_Source_df.drop("Rn")

# COMMAND ----------

Employee_Source_df.display()

# COMMAND ----------

#move to Raw zone
from delta.tables import *
LoadType ='I'
#LoadType ='F'
#exists = DeltaTable.isDeltaTable(spark, "/mnt/raw/Employee")
if LoadType == 'I':
    print('Running merge')
    Employee_Target = DeltaTable.forPath(spark, '/mnt/raw/Employee') 
    (
       Employee_Target.alias('target')
      .merge(Employee_Source_df.alias('source') , "source.ID = target.ID")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute()
    )
else:
    Employee_Source_df.write.format('delta').option("overwriteSchema", "true").mode("overwrite").save("/mnt/raw/Employee")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employee_raw

# COMMAND ----------

# MAGIC %md 
# MAGIC Move data from landing  input to archive after successfully moving data from landing to raw

# COMMAND ----------

for i in dbutils.fs.ls('/mnt/landing/input/Employee'):
    
    dbutils.fs.mv(i[0], '/mnt/landing/archive/Employee/'+i[1])

# COMMAND ----------

# MAGIC %md Raw to Refine load

# COMMAND ----------

# MAGIC %sql 
# MAGIC DESCRIBE HISTORY Employee_raw

# COMMAND ----------


exists = DeltaTable.isDeltaTable(spark, "/mnt/refined/Employee")
#LoadType = 'F'
LoadType ='I'
if LoadType == 'I':
    print('Running merge')
    #Retrieve only incremental records
    
    #Retrieve the last commit version from target table
    Employee_refine_df= spark.read.format('delta').load("/mnt/refined/Employee")
    last_commit_version = Employee_refine_df.agg({"_commit_version": "max"}).collect()[0][0]
    
    #Retrieve the latest version of the change data feed table
    df = spark.sql ("DESCRIBE HISTORY Employee_raw")
    max_version = df.agg({"version": "max"}).collect()[0][0]
    
    #Retrieve data changes from last commit version to latest version in change data feed table
    Employee_Source_df = spark.sql("SELECT * FROM table_changes('Employee_raw', {}, {})".format(last_commit_version,max_version))
    Employee_Source_df.createOrReplaceTempView("Employee_Source_tbl")
    # We also will get latest version of the business key records
    Employee_Source_df = spark.sql("""
                                    select * 
                                    from (
                                            select *, row_number () over (partition by ID order by _commit_version desc) as Rn 
                                            from Employee_Source_tbl
                                            WHERE _change_type !='update_preimage'
                                        ) sub2
                                    where sub2.rn =1
                                """)
    #drop Rownumber field
    Employee_Source_df = Employee_Source_df.drop("Rn")
    Employee_Target = DeltaTable.forPath(spark, '/mnt/refined/Employee') 
    (
       Employee_Target.alias('target')
      .merge(Employee_Source_df.alias('source') , "source.ID = target.ID")
      .whenMatchedUpdateAll()
      .whenNotMatchedInsertAll()
      .execute()
    )
else:
    #after enabling the change data feed for delta table
    #Since we want to use Change Data feed, we need to capture the version number, and use it as watermark for incremental
    df = spark.sql ("DESCRIBE HISTORY Employee_raw")
    max_version = df.agg({"version": "max"}).collect()[0][0]
    
    Employee_Raw_df = spark.read.format('delta').load("/mnt/raw/Employee")
    Employee_Raw_df = Employee_Raw_df.withColumn("_commit_version", lit(max_version))
    Employee_Raw_df = Employee_Raw_df.withColumn("_change_type", lit('insert'))
    Employee_Raw_df = Employee_Raw_df.withColumn("_commit_timestamp", lit(current_timestamp()))
    Employee_Raw_df.write.format('delta').option("overwriteSchema", "true").mode("overwrite").save("/mnt/refined/Employee")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM table_changes('Employee_raw', 1, 2)

# COMMAND ----------

#check the refine table
Employee_refine_df = spark.read.format('delta').load("/mnt/refined/Employee")
Employee_refine_df.display()

# COMMAND ----------

# MAGIC %sql 
# MAGIC --CREATE TABLE Employee_delta USING DELTA LOCATION "/mnt/refined/Employee_delta";
# MAGIC drop table Employee_raw

# COMMAND ----------


df = spark.sql ("DESCRIBE HISTORY Employee_raw")
#df = changedatadf.select(max('version'))

max_version = df.agg({"version": "max"}).collect()[0][0]

# COMMAND ----------

# MAGIC 
# MAGIC %sql 
# MAGIC SELECT * FROM table_changes('Employee_raw', 2)
# MAGIC where _change_type = 'update_postimage'

# COMMAND ----------

df.write.format('delta').mode("overwrite").save("/mnt/refined/Employee_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Employee_delta_incr
# MAGIC AS
# MAGIC SELECT * FROM table_changes('Employee_delta', 3, 5) where 
# MAGIC _change_type = 'update_postimage'

# COMMAND ----------

df = spark.sql("select * from Employee_delta")
df.write.format('delta').mode("overwrite").save("/mnt/refined/Employee_silver")
spark.sql("CREATE  TABLE Employee_silver USING DELTA LOCATION '/mnt/refined/Employee_silver'")
spark.sql("ALTER TABLE Employee_silver SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO Employee_silver t USING Employee_delta_incr s ON s.ID = t.ID
# MAGIC         WHEN MATCHED  THEN UPDATE SET Weight = s.Weight
# MAGIC         WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Employee_silver

# COMMAND ----------


