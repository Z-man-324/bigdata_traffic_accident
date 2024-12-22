from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("US Accidents Top Cities Analysis") \
    .getOrCreate()

# 从hdfs中读取CSV数据集
hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# 统计每个城市的事故数量
city_counts = df.groupBy("City").count()

# 提取前20个城市
top_cities = city_counts.orderBy(col("count").desc()).limit(20)

# 将结果转换为Pandas DataFrame
top_cities_pd = top_cities.toPandas()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "city_distribution_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

top_cities.write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)

# 绘制柱状图
plt.figure(figsize=(12, 8))
plt.bar(top_cities_pd['City'], top_cities_pd['count'], color='skyblue')
plt.xlabel('City')
plt.ylabel('Number of Accidents')
plt.title('Top 20 Cities with Most Accidents')
plt.xticks(rotation=45, ha='right')  # 设置x轴上标签的显示角度和对齐方式
plt.tight_layout()  # 自动调整子图参数，使之填充整个图像区域
plt.show()

# 停止Spark会话
spark.stop()