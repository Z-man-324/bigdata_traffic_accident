from kafka import KafkaProducer
import json
import time
import pandas as pd

# 全局创建 KafkaProducer 实例
producer = KafkaProducer(bootstrap_servers='192.168.85.1:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def producer_demo():
    try:
        # 读取 CSV 文件
        df = pd.read_csv('US_Accidents_March23.csv') #直接读取csv文件
        # 从hdfs中读取文件
        # hdfs_path = "hdfs://localhost:9000/input/US_Accidents_March23.csv"
        #df = spark.read.csv(hdfs_path, header=True, inferSchema=True)
        
        # 遍历 DataFrame 中的每一行，发送消息到指定的 topic
        for index, row in df.iterrows():
            accident_data = {
                'City': row['City'],
                'Severity': row['Severity'],
                'Timestamp': row['Start_Time']
            }
            producer.send('us_accidents_topic', accident_data)
            print("Message sent successfully")
            time.sleep(1)  # 每秒发送一条消息
    except Exception as e:
        print(f"Failed to send message: {e}")

if __name__ == '__main__':
    try:
        producer_demo()
    finally:
        # 关闭生产者连接
        producer.close()