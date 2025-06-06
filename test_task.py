from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName('ProductCategory').getOrCreate()

products = spark.createDataFrame([
    (1, 'Хлеб'),
    (2, 'Молоко'),
    (3, 'Сыр'),
    (4, 'Кефир')
], ['product_id', 'product_name'])

categories = spark.createDataFrame([
    (1, 'Молочные'),
    (2, 'Хлебобулочные'),
    (3, 'Сыры')
], ['category_id', 'category_name'])

product_category = spark.createDataFrame([
    (1, 2),
    (2, 1),
    (3, 3)
], ['product_id', 'category_id'])

joined = products.join(product_category, 'product_id', 'left') \
                 .join(categories, 'category_id', 'left') \
                 .select('product_name', 'category_name')

result = joined.withColumn('category_name', col('category_name')) \
               .orderBy('product_name')

result.show()
