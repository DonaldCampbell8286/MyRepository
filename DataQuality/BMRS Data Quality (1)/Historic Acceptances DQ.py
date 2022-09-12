# Databricks notebook source
# MAGIC %sql
# MAGIC --UNIT TEST TABLES
# MAGIC CREATE TABLE IF NOT EXISTS HistoricAcceptances
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/BMRS/Electricity Balancing Acceptances.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select  * 
# MAGIC from HistoricAcceptances

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC Sample DQ check SQL, each individual test separated by union all will yield one row per test
# MAGIC This code can be formatted in a readable manner for testing
# MAGIC */
# MAGIC 
# MAGIC SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmuName))/COUNT(*) as score FROM HistoricAcceptances
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-196' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM HistoricAcceptances
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM HistoricAcceptances
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmuName)* 100.00 / (Select Count(*) From HistoricAcceptances)) as score FROM HistoricAcceptances WHERE SUBSTRING(bmuName,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmuName,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmuName,2,1) LIKE '_'

# COMMAND ----------

spark.sql("SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmuName))/COUNT(*) as score FROM HistoricAcceptances union all SELECT current_date as executionTime,'DQ-196' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM HistoricAcceptances union all SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM HistoricAcceptances union all SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmuName)* 100.00 / (Select Count(*) From HistoricAcceptances)) as score FROM HistoricAcceptances WHERE SUBSTRING(bmuName,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmuName,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmuName,2,1) LIKE '_' ").coalesce(1).write.format("org.apache.spark.sql.json").mode("overwrite").save("mnt/BMRS/DQ/HADQResult.json")

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
