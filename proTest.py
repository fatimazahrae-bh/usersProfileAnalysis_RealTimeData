import findspark
findspark.init()
from cassandra.auth import PlainTextAuthProvider

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, concat_ws, current_date, datediff, when, sha2
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.types import StringType
from pyspark.sql.functions import sha2
from cassandra.cluster import Cluster


# Initialize a Spark session


spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config('spark.cassandra.connection.host', 'localhost') \
    .getOrCreate()

# Define a schema for the JSON data
json_schema = StructType([
    StructField("gender", StringType(), True),
    StructField("name", StructType([
        StructField("first", StringType(), True),
        StructField("last", StringType(), True),
    ]), True),
    StructField("location", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("country", StringType(), True),
        StructField("postcode", IntegerType(), True),
    ]), True),
    StructField("email", StringType(), True),
    StructField("login", StructType([
        StructField("uuid", StringType(), True),
        StructField("username", StringType(), True),
        StructField("password", StringType(), True),
    ]), True),
    StructField("dob", StructType([
        StructField("date", IntegerType(), True),
        StructField("age", IntegerType(), True),
    ]), True),
    StructField("registered", StructType([
        StructField("date", StringType(), True),
        StructField("age", IntegerType(), True),
    ]), True),
    StructField("phone", StringType(), True),
    StructField("cell", StringType(), True),
    StructField("id", StructType([
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
    ]), True),
    StructField("picture", StructType([
        StructField("large", StringType(), True),
        StructField("medium", StringType(), True),
        StructField("thumbnail", StringType(), True),
    ]), True),
    StructField("nat", StringType(), True),
])

# Read data from Kafka topic
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "user_profiles") \
    .load()

# Deserialize the JSON data and apply the schema
value_df = kafka_df.selectExpr("CAST(value AS STRING)")
parsed_df = value_df.select(from_json(value_df.value, json_schema).alias("data")).select("data.*")

# Transformation 1: Construire le nom complet
parsed_df = parsed_df.withColumn("full_name", concat_ws(" ", col("name.first"), col("name.last")))

# # Transformation 2: Valider ou recalculer l'âge
# current_date_col = current_date()
# parsed_df = parsed_df.withColumn("valid_age", 
#     when(col("dob.age") >= 0, col("dob.age"))
#     .otherwise(datediff(current_date_col, col("dob.date")) / 365).cast(IntegerType()))

# Transformation 3: Construire une adresse complète
parsed_df = parsed_df.withColumn("full_address", 
    concat_ws(", ", col("location.city"), col("location.state"), col("location.country")))



# Supprimer les colonnes "name" et "location"
parsed_df = parsed_df.drop("name", "location")

# Sélectionnez uniquement les colonnes nécessaires
parsed_df = parsed_df.selectExpr("login.uuid as id","gender", "full_name", "full_address", "email","login", "registered", "phone", "cell", "picture", "nat")



# Chiffrez les données des colonnes avec SHA-256
parsed_df = parsed_df.withColumn("phone", sha2(col("phone"), 256))
parsed_df = parsed_df.withColumn("cell", sha2(col("cell"), 256))
# parsed_df = parsed_df.withColumn("password", sha2(col("login.password"), 256))
parsed_df = parsed_df.withColumn("email", sha2(col("email"), 256))

# Vous pouvez maintenant supprimer les colonnes d'origine si vous le souhaitez
#parsed_df = parsed_df.drop("phone", "cell", "password", "email")



# # Start the query to process the data
# query = parsed_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("format", "json") \
#     .start()

# query.awaitTermination()





# def write_to_cassandra(parsed_df, id):
#     parsed_df.write \
#         .format("org.apache.spark.sql.cassandra") \
#         .mode("append") \
#         .options(
#             table="user_profiles",
#             keyspace="mon_keyspace"
#         ) \
#         .save()

# Écrivez les données du DataFrame en streaming dans Cassandra en utilisant foreachBatch
# query = parsed_df.writeStream \
#     .foreachBatch(write_to_cassandra) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()


# parsed_df.writeStream \
#     .outputMode("append") \
#     .format("org.apache.spark.sql.cassandra") \
#     .option("checkpointLocation", "") \
#     .option("keyspace", 'mon_keyspace') \
#     .option("table", 'user_profiles') \
#     .start()




# cassandra_write_options = {
# "keyspace": 'mon_keyspace',
# "table": 'user_profiles',
# }
# query2 = parsed_df.writeStream \
# .outputMode("append") \
# .format("org.apache.spark.sql.cassandra") \
# .option("checkpointLocation", "./checkpoint/data") \
# .options(**cassandra_write_options) \
# .start()
# query2.awaitTermination()

#connection with cassandra
#connect_to_cassandra
auth_provider = PlainTextAuthProvider(username='', password='')
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()

# keyspace= 'mon_keyspace'
# table= 'user_profiles_3'
# #create_cassandra_table
# create_table_query = f"""
# CREATE TABLE IF NOT EXISTS {keyspace}.{table} (
#     id UUID PRIMARY KEY,
#     gender TEXT,
#     email TEXT,
#     full_name TEXT,
#     full_address TEXT,
#     login TEXT,
#     registered TEXT,
#     phone TEXT,
#     cell  TEXT,
#     picture TEXT,
#     nat TEXT
# )
# """
#"uuid","gender", "full_name", "full_address", "email","login", "registered", "phone", "cell", "picture", "nat"
# session.execute(create_table_query)


# cassandra_write_options = {
#     "keyspace": 'mon_keyspace',
#     "table": 'user_profiles',
#     "outputMode": "append",
#     "checkpointLocation": "./checkpoint/data",
#     "cassandra.connection.host": "your_cassandra_host",  # Remplacez par l'adresse de votre nœud Cassandra
#     "cassandra.auth.username": "",  # Si l'authentification est activée
#     "cassandra.auth.password": "",  # Si l'authentification est activée
# }

query = parsed_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", './checkpoint/data') \
    .option("keyspace", 'mon_keyspace') \
    .option("table", 'user_profiles') \
    .start()

query.awaitTermination()



# query = parsed_df.writeStream \
#         .foreachBatch(lambda batchDF, batchId: batchDF.write \
#             .format("org.apache.spark.sql.cassandra") \
#             .option("checkpointLocation", './checkpoint/data') \
#             .option("keyspace", 'mon_keyspace') \
#             .option("table", 'user_profiles') \
#             .mode("append") \
#             .save()
#         ) \
#         .outputMode("append") \
#         .start()

# query = parsed_df.writeStream.outputMode("append").format("console").start()
# query.awaitTermination()


