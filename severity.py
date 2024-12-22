from pyspark import SparkContext
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt

# 初始化Spark会话和StreamingContext
spark = SparkSession.builder.master("local").appName("US Accidents Real-Time Severity Analysis").getOrCreate()
sc = spark.sparkContext

lines = sc.textFile("hdfs://localhost:9000/input/US_Accidents_March23.csv")


# 获取第一行（表头）并丢弃
header = lines.first()

# 跳过第一行，从第二行开始处理数据
lines = lines.filter(lambda line: line != header)


def parse_csv_line(line):
    # 根据CSV格式来解析字段
    values = line.split(',')
    return (values[2], 1)
severity_counts = lines.map(parse_csv_line).reduceByKey(lambda a, b: a + b)


# 计算总数
total_counts = severity_counts.map(lambda x: x[1]).sum()

# 计算占比
severity_percentages = severity_counts.map(lambda x: (x[0], x[1] / total_counts))

# 打印前10个严重性占比
print(severity_percentages.take(10))

# 将结果转换为Pandas DataFrame
severity_pd = spark.createDataFrame(severity_percentages).toPandas()

# MySQL 数据库连接信息
jdbc_hostname = "localhost"
jdbc_port = 3306
jdbc_database = "ysy_test"
jdbc_url = f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}?useSSL=false&serverTimezone=UTC"
jdbc_table = "severity_table"
jdbc_properties = {
    "user": "root",
    "password": "ysy13545372728",
    "driver": "com.mysql.cj.jdbc.Driver"
}


# 将 DataFrame 写入到 MySQL

severity_percentages.take(10).write.jdbc(url=jdbc_url, table=jdbc_table, mode="overwrite", properties=jdbc_properties)


# 将百分比字符串转换为浮点数
severity_pd['_2'] = severity_pd['_2'].astype(str).str.rstrip('%').astype(float)

# 绘制饼状图
plt.figure(figsize=(10, 7))
colors = ['#ff9999','#66b3ff','#99ff99','#ffcc99']  # 自定义颜色
explode = (0.1, 0, 0, 0)  # 突出显示第一个扇区

plt.pie(severity_pd['_2'], labels=severity_pd['_1'], colors=colors, explode=explode, autopct='%1.1f%%', startangle=140, shadow=True)
plt.title('Severity Proportion of Accidents', fontsize=16)  # 设置标题字体大小
plt.axis('equal')  
plt.show()


print(severity_counts.take(10))
spark.stop()