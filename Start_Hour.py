from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col
import matplotlib.pyplot as plt
import pandas as pd

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("US Accidents Hourly Analysis") \
    .getOrCreate()

# 从hdfs中读取CSV数据集
hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)


# 确保Start_Time是日期时间类型
df = df.withColumn("Start_Time", col("Start_Time").cast("timestamp"))

# 提取小时信息
df_with_hour = df.withColumn("Hour", hour("Start_Time"))

# 统计每小时事故数量
hourly_counts = df_with_hour.groupBy("Hour").count().orderBy("Hour")

# 将结果转换为Pandas DataFrame
hourly_pd = hourly_counts.toPandas()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "start_hour_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

hourly_counts.write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)


# 绘制折线图
plt.figure(figsize=(14, 7))
plt.plot(hourly_pd['Hour'], hourly_pd['count'], marker='o')
plt.title('Hourly Accident Distribution')
plt.xlabel('Hour of Day')
plt.ylabel('Number of Accidents')
plt.grid(True)
plt.show()

# 停止Spark会话
spark.stop()