import model.BankTransactionSchema.schema
import transform.BankTransactionTransformer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object KafkaSparkConsumer {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName("KafkaSparkConsumer")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")


        // ------------------------------------------------------------
        // 1. Read from Kafka
        // ------------------------------------------------------------

        val kafkaDf = spark.readStream
            .format("kafka")
            .option(
                "kafka.bootstrap.servers",
                "localhost:9092"
            )
            .option(
                "subscribe",
                "bank.transactions"
            )
            .option(
                "startingOffsets",
                "earliest"
            )
            .load()


        // ------------------------------------------------------------
        // 2. Extract Kafka metadata + raw JSON
        // ------------------------------------------------------------

        val parsedDf = kafkaDf
            .select(
                col("key")
                    .cast("string")
                    .alias("kafka_customer_id"),

                col("value")
                    .cast("string")
                    .alias("event_json"),

                col("topic")
                    .alias("kafka_topic"),

                col("partition")
                    .alias("kafka_partition"),

                col("offset")
                    .alias("kafka_offset"),

                col("timestamp")
                    .alias("kafka_timestamp")
            )
            .withColumn(
                "event",
                from_json(
                    col("event_json"),
                    schema
                )
            )


        // ------------------------------------------------------------
        // 3. Flatten envelope + payload
        // ------------------------------------------------------------

        val transactionsDf = parsedDf
            .select(
                col("kafka_customer_id"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),

                col("event.dataset")
                    .alias("dataset"),

                col("event.emitted_at")
                    .alias("emitted_at"),

                col("event.sequence")
                    .alias("sequence"),

                col("event.payload.account_id")
                    .alias("account_id"),

                col("event.payload.amount")
                    .alias("amount"),

                col("event.payload.balance_after")
                    .alias("balance_after"),

                col("event.payload.channel")
                    .alias("channel"),

                col("event.payload.currency")
                    .alias("currency"),

                col("event.payload.customer_id")
                    .alias("customer_id"),

                col("event.payload.customer_phone")
                    .alias("customer_phone"),

                col("event.payload.event_id")
                    .alias("event_id"),

                col("event.payload.event_time")
                    .alias("event_time"),

                col("event.payload.location")
                    .alias("location"),

                col("event.payload.merchant_category")
                    .alias("merchant_category"),

                col("event.payload.merchant_id")
                    .alias("merchant_id"),

                col("event.payload.merchant_name")
                    .alias("merchant_name"),

                col("event.payload.transaction_id")
                    .alias("transaction_id"),

                col("event.payload.transaction_type")
                    .alias("transaction_type")
            )


        // ------------------------------------------------------------
        // 4. Transform
        // ------------------------------------------------------------

        val transformedDf =
            BankTransactionTransformer.transform(
                transactionsDf
            )


        // ------------------------------------------------------------
        // 5. Final output schema
        // ------------------------------------------------------------

        val finalDf = transformedDf
            .select(
                col("customer_id"),
                col("transaction_id"),
                col("event_id"),

                col("event_time"),
                col("event_date"),

                col("emitted_at"),
                col("kafka_timestamp"),

                col("event_lag_seconds"),

                col("amount"),
                col("currency"),

                col("transaction_type"),
                col("merchant_category"),
                col("merchant_name"),

                col("kafka_customer_id"),
                col("kafka_partition"),
                col("kafka_offset")
            )


        // ------------------------------------------------------------
        // 6. Write to Parquet
        // ------------------------------------------------------------

        val query = finalDf
            .writeStream
            .format("parquet")
            .outputMode("append")
            .option(
                "path",
                "/tmp/ucando/data/bank_transactions"
            )
            .option(
                "checkpointLocation",
                "/tmp/ucando/checkpoints/bank-transactions-parquet"
            )
            .partitionBy("event_date")
            .start()

        query.awaitTermination()
    }
}