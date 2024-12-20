from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round as spark_round

spark = SparkSession.builder.appName("Homework_Spark").getOrCreate()

# 1. Завантаження даних
users_df = spark.read.csv("users.csv", header=True)
purchases_df = spark.read.csv("purchases.csv", header=True)
products_df = spark.read.csv("products.csv", header=True)

# 2. Видалення пропущених значень
users_df = users_df.dropna()
purchases_df = purchases_df.dropna()
products_df = products_df.dropna()

# # 3. Загальна сума покупок за кожною категорією продуктів
# Join purchases та products
purchases_with_products = purchases_df.join(products_df, "product_id")

# Add column total_cost
purchases_with_products = purchases_with_products.withColumn(
    "total_cost", col("quantity") * col("price")
)

# Загальна сума покупок за категоріями
total_by_category = (
    purchases_with_products.groupBy("category")
    .agg(spark_sum("total_cost").alias("total_sum"))
    .orderBy(col("total_sum").desc())
)
total_by_category.show()

# 4. Сума покупок для 18-25
# Filter by age
users_18_25 = users_df.filter((col("age") >= 18) & (col("age") <= 25))

# Join users and purchases
purchases_by_age = purchases_with_products.join(users_18_25, "user_id")

# Сума покупок за категоріями для 18-25
total_by_category_18_25 = (
    purchases_by_age.groupBy("category")
    .agg(spark_sum("total_cost").alias("total_sum"))
    .orderBy(col("total_sum").desc())
)
total_by_category_18_25.show()

# 5. Частка покупок за категоріями від загальної суми (18-25)
# Загальна сума покупок для 18-25
total_sum_18_25 = purchases_by_age.agg(spark_sum("total_cost").alias("total")).collect()[0]["total"]

# Частка покупок
category_share_18_25 = total_by_category_18_25.withColumn(
    "percentage", spark_round((col("total_sum") / total_sum_18_25) * 100, 2)
)
category_share_18_25.show()

# 6. Топ-3 категорії за відсотком витрат
top_3_categories = category_share_18_25.orderBy(col("percentage").desc()).limit(3)
top_3_categories.show()

spark.stop()
