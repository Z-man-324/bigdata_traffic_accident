from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("US Accidents Top Streets Analysis") \
    .getOrCreate()

# 从hdfs中读取CSV数据集
hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# 统计每个街道的事故数量
street_counts = df.groupBy("Street").count()

# 提取前20个街道
top_streets = street_counts.orderBy(col("count").desc()).limit(20)

# 将结果转换为Pandas DataFrame
top_streets_pd = top_streets.toPandas()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "street_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

top_streets.write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)

# 绘制横向条形图
plt.figure(figsize=(12, 8))
plt.barh(top_streets_pd['Street'], top_streets_pd['count'], color='skyblue')
plt.xlabel('Number of Accidents')
plt.title('Top 20 Streets with Most Accidents')
plt.gca().invert_yaxis()  # 反转y轴，使得条形图从上到下排列
plt.show()

# 停止Spark会话
spark.stop()