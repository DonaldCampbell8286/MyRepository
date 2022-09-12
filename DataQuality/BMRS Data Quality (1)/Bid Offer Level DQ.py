# Databricks notebook source
# MAGIC %sql
# MAGIC --UNIT TEST TABLES
# MAGIC CREATE TABLE IF NOT EXISTS ElectricityBalancingBidsandOffers
# MAGIC   USING DELTA
# MAGIC   LOCATION '/mnt/BMRS/Electricity Balancing Bids and Offers.delta'

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC 
# MAGIC select  *
# MAGIC from ElectricityBalancingBidsandOffers

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score FROM ElectricityBalancingBidsandOffers WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_'

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers%sql
# MAGIC (100.00*COUNT(offerPrice))/COUNT(*)%sql
# MAGIC 
# MAGIC Sample DQ check SQL, each individual test separated by union all will yield one row per test
# MAGIC This code can be formatted in a readable manner for testing
# MAGIC */
# MAGIC 
# MAGIC SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmUnitID))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-196' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score FROM ElectricityBalancingBidsandOffers WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_'

# COMMAND ----------

spark.sql("SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount (100.00*COUNT(bmUnitID))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-196' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score from ElectricityBalancingBidsandOffers where SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_' ").coalesce(1).write.format("org.apache.spark.sql.json").mode("overwrite").save("mnt/BMRS/DQ/BOLDQResult.json")

# COMMAND ----------

SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmUnitID))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
union all
SELECT current_date as executionTime,'DQ-196' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
union all
SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
union all
SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score FROM ElectricityBalancingBidsandOffers WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_'

# COMMAND ----------

spark.sql("SELECT current_date as executionTime,'DQ-218' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bmUnitID))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-196' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-195' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score FROM ElectricityBalancingBidsandOffers WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_' ").coalesce(1).write.format("org.apache.spark.sql.json").mode("overwrite").save("mnt/BMRS/DQ/BOLDQResult.json")

# COMMAND ----------


