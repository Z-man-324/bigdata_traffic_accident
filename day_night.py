from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("US Accidents Day/Night Analysis") \
    .getOrCreate()

# 从hdfs中读取CSV数据集
hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# 定义白天和晚上的时间段，并创建新列 'Day_Night'
df = df.withColumn("Day_Night", when((col("Civil_Twilight") == "Day"), "Day")
                              .otherwise("Night"))

# 统计白天和晚上的事故数量
day_night_counts = df.groupBy("Day_Night").count()

# 将结果转换为Pandas DataFrame
day_night_pd = day_night_counts.toPandas()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "day_night_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

day_night_counts.write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)


# 绘制饼状图
plt.figure(figsize=(8, 8))
plt.pie(day_night_pd['count'], labels=day_night_pd['Day_Night'], autopct='%1.1f%%', startangle=140)
plt.title('Distribution of Accidents by Day and Night')
plt.axis('equal') 
plt.show()

# 停止Spark会话
spark.stop()