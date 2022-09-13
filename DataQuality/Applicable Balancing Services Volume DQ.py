# Databricks notebook source
# MAGIC %sql
# MAGIC --UNIT TEST TABLES
# MAGIC CREATE TABLE IF NOT EXISTS ApplicableBalancingServicesVolume
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/BMRS/Applicable Balancing Services Volume Data.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select  * 
# MAGIC from ApplicableBalancingServicesVolume
# MAGIC WHERE bmUnitType = 'S'

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Sample DQ check SQL, each individual test separated by union all will yield one row per test
# MAGIC This code can be formatted in a readable manner for testing
# MAGIC DATASET = Applicable Balancing Services Volume
# MAGIC */
# MAGIC 
# MAGIC SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmUnitID))/COUNT(*) as score FROM ApplicableBalancingServicesVolume
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-185' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ApplicableBalancingServicesVolume)) as score FROM ApplicableBalancingServicesVolume WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_'

# COMMAND ----------


1
'''
2
final step: run a python command to take the SQL from above and push the output to DataLake in a JSON format ready for ingestion to Axon
3
Note: you will have to undo the formatting from the previous step, also note the quote marks are different here because this is a python cell, not a SQL one!
4
'''
5
 
6
spark.sql("SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmUnitID))/COUNT(*) as score FROM ApplicableBalancingServicesVolume union all SELECT current_date as executionTime,'DQ-185' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ApplicableBalancingServicesVolume)) as score FROM ApplicableBalancingServicesVolume WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_'").coalesce(1).write.format("org.apache.spark.sql.json").mode("overwrite").save("mnt/BMRS/DQ/ABSVDQResult.json")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC select * from ApplicableBalancingServicesVolume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ApplicableBalancingServicesVolumeData
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/BMRS/Applicable Balancing Services Volume Data.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ApplicableBalancingServicesVolumeData
# MAGIC order by settDate desc

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS ElectricityBalancingAcceptances
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/BMRS/Electricity Balancing Acceptances.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC select settPeriod, count(*) from ElectricityBalancingAcceptances
# MAGIC group by settPeriod
# MAGIC order by 1
