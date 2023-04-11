from pyspark.sql import SparkSession
from pyspark.sql.functions import rand

spark = SparkSession.builder \
    .appName("ReadJson") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

df = spark.read.json("hdfs://localhost:9000/data/houses.json")



def transfrom(df):
    df = df.filter(df.size < 1000)
    df = df.filter(df.width < 50)
    df = df.filter(df.length < 200)

    df = df.filter(df.latitude < 20)
    df = df.filter(df.latitude > 10)

    df = df.filter(df.longitude > 106)
    df = df.filter(df.longitude < 107)
    return df

df = transfrom(df)

df.printSchema()
df.show(10)
print("Số lượng bản ghi của dataframe kết quả:", df.count())

final_name = 'clean_houses.json'
df.write.mode("overwrite").json(f"hdfs://localhost:9000/data/{final_name}")
df.coalesce(1).write.mode("overwrite").format('json').save(final_name)

import os
json_file = [f for f in os.listdir(final_name) if f.endswith('.json')][0]
json_file = f'{final_name}/{json_file}'

from s3 import s3Action
client = s3Action()
client.upload(json_file, final_name)


