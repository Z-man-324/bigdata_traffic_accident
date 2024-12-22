from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("US Accidents Severity by City Analysis") \
    .getOrCreate()

# 从hdfs中读取CSV数据集
hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# 统计每个城市的严重性等级数量
severity_city_counts = df.groupBy("City", "Severity").count().withColumnRenamed("count", "severity_count")

# 计算每个城市的事故总数
city_total_counts = df.groupBy("City").count().withColumnRenamed("count", "total_count")

# 将城市总数与严重性等级数量合并，并计算每个城市的严重性占比
severity_city_percentages = severity_city_counts.join(city_total_counts, "City") \
    .withColumn("percentage", (col("severity_count") / col("total_count") * 100))

# 假设严重性等级是有限的几个值，比如1, 2, 3, 4（这里需要根据实际数据调整）
severities = [1, 2, 3, 4]  # 假设这是严重性等级的所有可能值

# 将结果转换为Pandas DataFrame，并处理以确保每个城市都有四个严重性等级的数据
severity_city_pd = severity_city_percentages.toPandas()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "city_severity_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

severity_city_percentages.write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)

# 使用pivot_table来确保每个城市都有四个严重性等级的数据，用0填充缺失值
severity_pivot = severity_city_pd.pivot_table(index='City', columns='Severity', values='percentage', aggfunc='sum', fill_value=0)

# 选择事故数量（或严重性占比总和）最多的前10个城市
# 注意：这里我们使用severity_pivot的sum()作为排序依据，因为pivot_table已经为每个城市计算了所有严重性的占比
top_cities = severity_pivot.sum(axis=1).nlargest(10).index

# 准备绘制堆叠柱状图的数据
stacked_data = severity_pivot.loc[top_cities]
stacked_data = stacked_data.reindex(columns=severities)  # 确保列的顺序是正确的

# 绘制堆叠柱状图
plt.figure(figsize=(12, 8))
stacked_data.plot(kind='bar', stacked=True, color=['blue', 'green', 'red', 'purple'])
plt.xlabel('City')
plt.ylabel('Severity Percentage')
plt.title('Severity Percentage Stacked Bar Chart in Top 10 Cities')
plt.xticks(rotation=45)
plt.legend(title='Severity', labels=[f'Severity {i}' for i in severities])  # 添加图例
plt.tight_layout()  # 自动调整子图参数, 使之填充整个图像区域
plt.show()

# 停止Spark会话
spark.stop()