from pyspark.sql.functions import *
import logging
import sys

logger = logging.getLogger('py4j')
sh = logging.StreamHandler(sys.stdout)
sh.setLevel(logging.DEBUG)
logger.addHandler(sh)

from pyspark.sql import SparkSession
input_path = sys.argv[1]
outpu_path = sys.argv[2]

spark = SparkSession \
    .builder \
    .appName("Cargill challenge") \
    .getOrCreate()

# Remove logs INFO during Spark JOB
spark.sparkContext.setLogLevel("ERROR")

# 1 Import the file named ks-projects-201612.csv (You can find this file on kaggle: https://www.kaggle.com/kemical/kickstarter-projects/download)
df = spark.\
    read.option("header", "true").\
    option("quote", "\"").\
    option("escape", "\"").\
    option("inferSchema", "true").\
    csv(input_path)
# Organize each column in the file that is delimited by commas

df = df.withColumnRenamed("ID ","ID").\
    withColumnRenamed("name ","name").\
    withColumnRenamed("category ","category").\
    withColumnRenamed("main_category ", "main_category").\
    withColumnRenamed("currency ","currency").\
    withColumnRenamed("deadline ","deadline").\
    withColumnRenamed("goal ","goal").\
    withColumnRenamed("launched ","launched").\
    withColumnRenamed("pledged ","pledged").\
    withColumnRenamed("state ","state").\
    withColumnRenamed("backers ","backers").\
    withColumnRenamed("country ","country").\
    withColumnRenamed("usd pledged ","usd pledged")
# Arrange the name spaces of each column

# 2 Create a dataframe with a column named as category and three entries: Games, Food, Music
df2 = spark.createDataFrame([('Games',),('Food',),('Music',)],('category',))

# 3 Join the created dataframe with the data from the text file
df_joiner = df.join(df2,df["category"]==df2["category"], 'inner').drop(df2['category'])

df_joiner = df_joiner.withColumn('launched',date_format(df_joiner.launched.cast('timestamp'), 'yyyy-MM-dd HH:mm:ss').cast('timestamp'))
# Change the column datatype to its default. 

df_joiner = df_joiner.withColumn("pledged", col("pledged").cast('float'))
# Change the column datatype to its default.

logger.critical("# 4 Use this dataframe to answer the following questions")
logger.critical("# 4.1 Which is the older project fully supported)")
df_joiner.select("name","launched").orderBy(asc("launched")).show(1, False)

logger.critical("# 4.2 Which is the main category with the most pledged ammount")
df_joiner.groupBy("category").agg(sum(col("pledged").cast("Decimal(18,2)")).alias("pledged")).orderBy(asc("pledged")).show(1)


logger.critical("# 4.3 What is the average pledged ammount in USD for each category")
df_joiner.\
    where("trim(currency) == 'USD'").\
    groupBy("category").\
    agg(avg(col("pledged").cast("Decimal(18,2)")).alias("pledged")).show()

logger.critical("# 5 Save the US projects from the categories listed on our created dataframe in a csv file")
eua_country = df_joiner.where("trim(country) == 'US'")
eua_country.repartition(1).write.format("csv").save(output_path, sep='|')

# Create a data frame with projects in the USA from the categories listed after saving them in a .csv file 

