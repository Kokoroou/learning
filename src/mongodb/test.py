import pymongo
import pyspark
from pyspark.sql import SparkSession, Row
from config import host


if __name__ == "__main__":
    # client = pymongo.MongoClient(
    #     "mongodb+srv://bigdata:OrhluidhUvCUg16r@cluster0.r6n8sdi.mongodb.net/?retryWrites=true&w=majority")
    # db = client.test
    # print(db)

    # Config
    database = "BigData"
    collection = "Loan"
    server = "mongodb+srv://bigdata:OrhluidhUvCUg16r@cluster0.r6n8sdi.mongodb.net"
    config = "?retryWrites=true&w=majority"
    connectionString = 'mongodb+srv://bigdata:OrhluidhUvCUg16r@cluster0.r6n8sdi.mongodb.net/?retryWrites=true' \
                       '&w=majority '
    uri = f"{server}/{database}.{collection}{config}"

    # Initialize session config to connect later
    spark = SparkSession \
        .builder \
        .appName("MongoDBTest") \
        .master('local[*, 4]') \
        .config("spark.mongodb.input.uri", connectionString) \
        .config("spark.mongodb.output.uri", connectionString) \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .getOrCreate()
    #     .config("spark.mongodb.database", database) \
    #     .config("spark.mongodb.collection", collection) \

    # Pipeline to convert upcoming data to table
    pipeline = "[ \
        {'$project': { \
            UserID:1, \
            ScoreCard:1, \
            CreditRisk:1, \
            _id:0 \
            } \
        }]"

    # Read data
    data_table = spark.read \
        .format("mongo") \
        .option("database", database) \
        .option("collection", collection) \
        .option("pipeline", pipeline) \
        .option("partitioner", "MongoSinglePartitioner") \
        .load()

    data_table.printSchema()


