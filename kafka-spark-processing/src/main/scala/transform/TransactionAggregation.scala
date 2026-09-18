package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object TransactionAggregation {
    def aggregate(
        transactionsDf: DataFrame
    ): DataFrame = {
        transactionsDf
        .withWatermark(
            "event_time",
            "10 minutes"
        )
        .groupBy(
            window(
            col("event_time"),
            "5 minutes"
            ),
            col("customer_id")
        )
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("average_amount")
        )
        .select(
            col("customer_id"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("transaction_count"),
            col("total_amount"),
            col("average_amount")
        )
    }
}