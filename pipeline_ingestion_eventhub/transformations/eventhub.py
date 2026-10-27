from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark import pipelines as dp

EH_CONN_STR=spark.conf.get("EH.CONN.STR")
EH_NAME=spark.conf.get("EH.NAME")
EH_NAMESPACE=spark.conf.get("EH.NAMESPACE")

KAFKA_OPTIONS = {
    "kafka.bootstrap.servers": f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe": EH_NAME,
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.jaas.config": f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{EH_CONN_STR}";',
    "kafka.request.timeout.ms": "60000",
    "kafka.session.timeout.ms": "30000",
    "maxOffsetsPerTrigger": "50000",
    "failOnDataLoss": "true",
    "startingOffsets": "earliest",
}


ORDER_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("restaurant_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_type", StringType(), True),
    StructField("items", StringType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("payment_method", StringType(), True),
    StructField("order_status", StringType(), True)
])


# Streaming table: reads Event Hubs incrementally; checkpoints are managed by the pipeline.
@dp.table(name="orders", table_properties={"quality": "bronze"})
def orders():
    return (
        spark.readStream
        .format("kafka")
        .options(**KAFKA_OPTIONS)
        .load()
        .withColumn("data", F.from_json(F.col("value").cast("string"), ORDER_SCHEMA))
        .select("data.*")
        .withColumnRenamed("timestamp", "order_timestamp")
    )


# One-time backfill of historical orders into the same streaming table.
@dp.append_flow(target="orders", name="flow_eventhub_backfill", once=True)
def flow_eventhub_backfill():
    return spark.read.table("historical_orders").select(
        "order_id",
        F.col("order_timestamp").cast("string").alias("order_timestamp"),
        "restaurant_id",
        "customer_id",
        "order_type",
        "items",
        F.col("total_amount").cast("double").alias("total_amount"),
        "payment_method",
        "order_status",
    )

