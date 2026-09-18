package feature

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object CustomerSpendingFeatures{
    def build(transactionsDf: DataFrame): DataFrame = {
        transactionsDf
            .withWatermark(
                "event_time",
                "15 minutes"
            )
            .groupBy(
                window(
                    col("event_time"),
                    "15 minutes",
                    "5 minutes"
                ),
                col("customer_id")
            )
            .agg(
                sum("amount").alias("total_spend"),
                count("*").alias("transaction_count"),
                avg("amount").alias("avg_transaction_value"),
                avg("amount").alias("max_transaction_value"),
                approx_count_distinct("merchant_category").alias("merchant_category_count")
            )
            .withColumn(
                "high_value_transaction",
                col("max_transaction_value") >= 100000
            )
            .withColumn(
                "transaction_velocity",
                col("transaction_count") / 15.0
            )
            .withColumn(
                "spend_velocity",
                col("total_spend") / 15.0
            )
            .select(
                col("customer_id"),
                col("window.start").alias("window_start"),
                col("window.end").alias("window_end"),
                col("total_spend"),
                col("transaction_count"),
                col("avg_transaction_value"),
                col("max_transaction_value"),
                col("merchant_category_count"),
                col("transaction_velocity"),
                col("spend_velocity"),
                col("high_value_transaction")
            )
    }
}