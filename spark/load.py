from pyspark.sql import SparkSession
from pyspark.sql.functions import rand
# Khởi tạo SparkSession
spark = SparkSession.builder \
    .appName("ReadJson") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.executor.memoryOverhead", "2g") \
    .getOrCreate()

def cleanDf(df):
    select_list = ['account_id', 'account_name', 'ad_id', 'address', 'area', 'area_name', 'area_v2',
                   'body', 'category', 'category_name', 'commercial_type', 'date', 'detail_address',
                   'direction', 'floornumber', 'floors', 'house_type', 'image', 'latitude', 'longitude',
                   'location', 'living_size', 'list_id', 'list_time', 'price', 'region_name', 'rooms',
                   'size', 'street_name', 'street_number', 'subject', 'toilets', 'type', 'ward_name',
                   'width', 'length']
    df = df.select(select_list)
    return df


hdfs_url = 'hdfs://localhost:9000/houses/'
file_list = ['raw311222.json', 'raw100123.json', 'raw310323.json']

def combineDf():
    df = cleanDf(spark.read.json(hdfs_url + file_list[0]))
    for file_name in file_list[1:]:
        url = hdfs_url + file_name
        temp_df = spark.read.json(url)
        temp_df = cleanDf(temp_df)
        df = df.union(temp_df)
        print(f"Số lượng bản ghi của {file_name} kết quả:", temp_df.count())

    df = df.dropDuplicates(["ad_id"])
    return df

df = combineDf()
df.printSchema()

print("Số lượng bản ghi của dataframe kết quả:", df.count())


df.write.json("hdfs://localhost:9000/data/houses.json")