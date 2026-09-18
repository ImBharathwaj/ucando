package transform

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object BankTransactionTransformer {

    def transform(df: DataFrame): DataFrame = {

        df
            // --------------------------------------------------------
            // Convert application timestamps
            // --------------------------------------------------------

            .withColumn(
                "event_time",
                to_timestamp(
                    col("event_time"),
                    "yyyy-MM-dd'T'HH:mm:ss"
                )
            )

            .withColumn(
                "emitted_at",
                to_timestamp(
                    col("emitted_at"),
                    "yyyy-MM-dd'T'HH:mm:ss"
                )
            )

            // --------------------------------------------------------
            // Kafka timestamp
            // --------------------------------------------------------

            .withColumn(
                "kafka_timestamp",
                col("kafka_timestamp").cast("timestamp")
            )

            // --------------------------------------------------------
            // Event processing delay
            // --------------------------------------------------------

            .withColumn(
                "event_lag_seconds",
                col("emitted_at").cast("long") -
                    col("event_time").cast("long")
            )

            // --------------------------------------------------------
            // Physical partitioning column
            // --------------------------------------------------------

            .withColumn(
                "event_date",
                to_date(col("event_time"))
            )

            // --------------------------------------------------------
            // Basic data quality filtering
            // --------------------------------------------------------

            .filter(
                col("event_id").isNotNull &&
                col("transaction_id").isNotNull &&
                col("customer_id").isNotNull &&
                col("amount").isNotNull
            )
    }
}