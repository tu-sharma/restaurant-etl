from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark import pipelines as dp

@dp.materialized_view(
    name="d_sales_summary",
    partition_cols = ["order_date"],
    table_properties = {"qualtity": "gold"
    })
def d_sales_summary():
    df_daily_agg = dp.read("`02_silver`.fact_orders")\
                .groupBy("order_date")\
                    .agg(
                        countDistinct(col("order_id")).alias("total_orders"),
                        sum(col("total_amount").cast("decimal(10,2)")).alias("total_revenue"),
                        avg(col("total_amount").cast("decimal(10,2)")).alias("avg_order_value"),
                        countDistinct("customer_id").alias("unique_customers"),
                        countDistinct("restaurant_id").alias("unique_restaurants"),
                        count(when(col("order_type") == "dine_in", col("order_id")).otherwise(None)).alias(
                            "dine_in_orders"
                        ),
                        count(when(col("order_type") == "takeaway", col("order_id")).otherwise(None)).alias(
                            "takeaway_orders"
                        ),
                        count(when(col("order_type") == "delivery", col("order_id")).otherwise(None)).alias(
                            "delivery_orders"
                        ),
                    )\
                    .select(
                        "order_date",
                        "total_orders",
                        "total_revenue",
                        "avg_order_value",
                        "unique_customers",
                        "unique_restaurants",
                        "dine_in_orders",
                        "takeaway_orders",
                        "delivery_orders",
                    )
    return df_daily_agg
