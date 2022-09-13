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
# MAGIC /*Dataset = Bid Offer Level*/
# MAGIC SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score FROM ElectricityBalancingBidsandOffers WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_'
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-187' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers
# MAGIC union all
# MAGIC SELECT current_date as executionTime,'DQ-189' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers

# COMMAND ----------

spark.sql("SELECT current_date as executionTime,'DQ-193' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (Count(bmUnitID)* 100.00 / (Select Count(*) From ElectricityBalancingBidsandOffers)) as score FROM ElectricityBalancingBidsandOffers WHERE SUBSTRING(bmUnitID,1,1) BETWEEN '0' AND '9' OR SUBSTRING(bmUnitID,1,1) BETWEEN 'A' AND 'Z' AND SUBSTRING(bmUnitID,2,1) LIKE '_' union all SELECT current_date as executionTime,'DQ-187' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(bidPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers union all SELECT current_date as executionTime,'DQ-189' as identity, 'Test' as dqReportProvider, count(*) as rowCount, (100.00*COUNT(offerPrice))/COUNT(*) as score FROM ElectricityBalancingBidsandOffers").coalesce(1).write.format("org.apache.spark.sql.json").mode("overwrite").save("mnt/BMRS/DQ/BOLDQResult.json")
