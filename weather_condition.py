from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import matplotlib.pyplot as plt
import pandas as pd

# 初始化Spark会话
spark = SparkSession.builder \
    .appName("US Accidents Weather Condition Analysis") \
    .getOrCreate()

# 从hdfs中读取CSV数据集
hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
df = spark.read.csv(hdfs_path, header=True, inferSchema=True)

# 统计每种天气状况的事故数量
weather_counts = df.groupBy("Weather_Condition").count()
top_weathers= weather_counts.orderBy(col("count").desc()).limit(30)

# 将结果转换为Pandas DataFrame
weather_pd = top_weathers.toPandas()
print(weather_pd)
weather_pd=weather_pd.dropna()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "weather_condition_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

top_weathers.write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)


plt.figure(figsize=(10, 8))
plt.barh(weather_pd['Weather_Condition'], weather_pd['count'], color='skyblue')
plt.xlabel('Number of Accidents')
plt.title('Distribution of Accidents by Weather Condition')
plt.gca().invert_yaxis()  # 反转y轴，使得条形图从上到下排列
plt.show()

# 停止Spark会话
spark.stop()
