from kafka import KafkaConsumer
from kafka.errors import NoOffsetForPartitionError
import matplotlib.pyplot as plt
import pandas as pd

# 初始化消费者
consumer = KafkaConsumer(
    bootstrap_servers=['192.168.85.1:9092'],
    auto_offset_reset='earliest',
    value_deserializer=lambda m: m.decode('utf-8'),
    group_id='severity_analysis_group'
)

# 订阅主题
consumer.subscribe(['us_accidents_topic'])

# 存储每个城市每种严重性的事故数量
severity_city_counts = {}

try:
    for message in consumer:
        accident_data = json.loads(message.value)
        city = accident_data['City']
        severity = accident_data['Severity']
        if city not in severity_city_counts:
            severity_city_counts[city] = {}
        severity_city_counts[city][severity] = severity_city_counts[city].get(severity, 0) + 1

finally:
    consumer.close()

# 将结果转换为Pandas DataFrame
severity_city_pd = pd.DataFrame(list(severity_city_counts.items()), columns=['City', 'Severity', 'Count'])

# 选择事故数量最多的前10个城市
top_cities = severity_city_pd.nlargest(10, "Count")

# 绘制堆叠条形图
plt.figure(figsize=(12, 8))
bottom = [0] * len(top_cities)
colors = ['red', 'green', 'blue', 'yellow']  # 为每个 Severity 等级定义颜色

# 为每个 Severity 等级绘制堆叠条形图
for i, severity in enumerate(top_cities['Severity'].unique()):
    city_counts = top_cities[top_cities['Severity'] == severity]
    plt.barh(city_counts['City'], city_counts['Count'], left=bottom, color=colors[i % len(colors)], label=severity)
    bottom = [b + c for b, c in zip(bottom, city_counts['Count'])]

plt.xlabel('City')
plt.ylabel('Number of Accidents')
plt.title('Number of Accidents by Severity in Top 10 Cities')
plt.legend(title='Severity')
plt.show()