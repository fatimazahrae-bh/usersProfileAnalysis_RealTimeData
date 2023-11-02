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


# spark = SparkSession.builder \
#     .appName("KafkaSparkIntegration") \
#     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,"
#             "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
#     .config('spark.cassandra.connection.host', 'localhost') \
#     .getOrCreate()

spark = SparkSession.builder \
    .appName("KafkaSparkIntegration") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.4,com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .config("spark.mongodb.output.uri", "mongodb://localhost:27017") \
    .config("spark.mongodb.output.database", "user_profiles") \
    .config("spark.mongodb.output.collection", "users") \
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
parsed_df = parsed_df.selectExpr("login.uuid as id","gender", "full_name", "full_address", "email", "registered", "phone", "cell", "picture", "nat")
#parsed_df = parsed_df.selectExpr("login.uuid as id","gender", "full_name", "full_address", "email","login", "registered", "phone", "cell", "picture", "nat")


from pyspark.sql.functions import from_json, col, concat_ws, current_date, datediff, when, sha2

# Chiffrez les données des colonnes avec SHA-256
parsed_df = parsed_df.withColumn("phone", sha2(col("phone"), 256))
parsed_df = parsed_df.withColumn("cell", sha2(col("cell"), 256))
# parsed_df = parsed_df.withColumn("password", sha2(col("login.password"), 256))
parsed_df = parsed_df.withColumn("email", sha2(col("email"), 256))

#---------------------------------Cassandra---------------------------------------------------------

#connect_to_cassandra
auth_provider = PlainTextAuthProvider(username='', password='')
cluster = Cluster(['localhost'], port=9042)
session = cluster.connect()


query = parsed_df.writeStream \
    .outputMode("append") \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", './checkpoint/data') \
    .option("keyspace", 'mon_keyspace') \
    .option("table", 'user_profiles') \
    .start()

query.awaitTermination()


#---------------------------------------MongoDB---------------------------------
query_mongodb = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(lambda batchDF, batchId: batchDF.write \
        .format("mongo") \
        .option("uri", 'mongodb://localhost:27017') \
        .option("database", 'user_profiles') \
        .option("collection", 'users') \
        .mode("append") \
        .save()
    ) \
    .start()

query_mongodb.awaitTermination()
