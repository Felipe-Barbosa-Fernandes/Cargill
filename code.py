from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *

# 1 Import the file named ks-projects-201612.csv (You can find this file on kaggle: https://www.kaggle.com/kemical/kickstarter-projects/download)
df = spark.read.option("header", "true").option("quote", "\"").option("escape", "\"").option("inferSchema", "true").csv("hdfs:///user/teste/ks-projects-201612.csv")
# Organize each column in the file that is delimited by commas

df = df.withColumnRenamed("ID ","ID").withColumnRenamed("name ","name").withColumnRenamed("category ","category").withColumnRenamed("main_category ", "main_category").withColumnRenamed("currency ","currency").withColumnRenamed("deadline ","deadline").withColumnRenamed("goal ","goal").withColumnRenamed("launched ","launched").withColumnRenamed("pledged ","pledged").withColumnRenamed("state ","state").withColumnRenamed("backers ","backers").withColumnRenamed("country ","country").withColumnRenamed("usd pledged ","usd pledged")
# Arrange the name spaces of each column

# 2 Create a dataframe with a column named as category and three entries: Games, Food, Music
df2 = spark.createDataFrame([('Games',),('Food',),('Music',)],('category',))

# 3 Join the created dataframe with the data from the text file
df_joiner = df.join(df2,df["category"]==df2["category"], 'inner').drop(df2['category'])

df_joiner = df_joiner.withColumn('launched',date_format(df_joiner.launched.cast('timestamp'), 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))
# Change the column datatype to its default. 

df_joiner = df_joiner.withColumn("pledged", col("pledged").cast('float'))
# Change the column datatype to its default.

# 4 Use this dataframe to answer the following questions
# 4.1 Which is the older project fully supported
df_joiner.select("name","launched").orderBy(asc("launched")).show(1, False)
# Help RIZ Make A Charity Album: 8 Songs, 8 Causes, 1 Song For Each Cause (Canceled)|1970-01-01 01:00:00|

# 4.2 Which is the main category with the most pledged ammount
df_joiner.groupBy("category").agg(sum(col("pledged").cast("Decimal(18,2)")).alias("pledged")).orderBy(asc("pledged")).show(1)
# Games|18975879.60|

# 4.3 What is the average pledged ammount in USD for each category  
df_joiner.where("trim(currency) == 'USD'").groupBy("category").agg(avg(col("pledged").cast("Decimal(18,2)")).alias("pledged")).show()
# Food|6497.381452|
# Games|8520.507846|
# Music|4652.214944|

# 5 Save the US projects from the categories listed on our created dataframe in a csv file
eua_country = df_joiner.where("trim(country) == 'US'")
eua_country.repartition(1).write.csv("hdfs:///user/teste/country_eua.csv", sep='|')
# Create a data frame with projects in the USA from the categories listed after saving them in a .csv file 

# 6 Tell us how would you save these entries on an Impala database
"""
I would save the SparkDataFrame content to the Impala database table via JDBC.
first, I need the URL in the Impala database.
For example:
url = "jdbc: impala: imp_env; auth = noSas"
Second, my steps for writing data using JDBC connections from the Impala database in PySpark:
(I can use the URL or the variable I created for the URL in this step)
df_joiner.write.mode ("attach") .jdbc (url = "jdbc: impala: //10.61.1.101: 21050 / test; auth = noSas", table = "ks_projects_201612", pro)
mode: one of the 'attach', 'replace', 'error' and 'ignore' modes.
URL: URL of the JDBC database in the jdbc: subprotocol: subname format.
tableName: the name of the table in the Impala database.
"""
