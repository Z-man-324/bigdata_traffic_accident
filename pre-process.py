import pandas as pd

# 加载CSV文件
file_path = 'US_Accidents_March23.csv'  
df = pd.read_csv(file_path)

# 输出数据集的总数据量
print("数据集总数据量:", len(df))

# 输出数据集的前几行
print("First few rows of the dataset:")
print(df.head())

# 检查重复值
print("重复值数量:", df.duplicated().sum())

# 删除重复值
df = df.drop_duplicates()

# 检查缺失值
print("缺失值数量:")
print(df.isnull().sum())

# 处理缺失值，用0填充
df = df.fillna(0)

# 保存预处理后的数据集
df.to_csv('processed_us_accidents.csv', index=False)