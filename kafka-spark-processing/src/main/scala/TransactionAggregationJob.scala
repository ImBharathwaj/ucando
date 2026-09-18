import model.BankTransactionSchema.schema
import transform.BankTransactionTransformer
import transform.TransactionAggregation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TransactionAggregationJob {

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .appName("TransactionAggregationJob")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val kafkaDf =
      spark.readStream
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
            "latest"
        )
        .load()

    val parsedDf =
      kafkaDf
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

    val transactionsDf =
      parsedDf
        .select(
          col("kafka_customer_id"),
          col("kafka_topic"),
          col("kafka_partition"),
          col("kafka_offset"),
          col("kafka_timestamp"),

          col("event.dataset").alias("dataset"),
          col("event.emitted_at").alias("emitted_at"),
          col("event.sequence").alias("sequence"),

          col("event.payload.account_id").alias("account_id"),
          col("event.payload.amount").alias("amount"),
          col("event.payload.balance_after").alias("balance_after"),
          col("event.payload.channel").alias("channel"),
          col("event.payload.currency").alias("currency"),
          col("event.payload.customer_id").alias("customer_id"),
          col("event.payload.customer_phone").alias("customer_phone"),
          col("event.payload.event_id").alias("event_id"),
          col("event.payload.event_time").alias("event_time"),
          col("event.payload.location").alias("location"),
          col("event.payload.merchant_category").alias("merchant_category"),
          col("event.payload.merchant_id").alias("merchant_id"),
          col("event.payload.merchant_name").alias("merchant_name"),
          col("event.payload.transaction_id").alias("transaction_id"),
          col("event.payload.transaction_type").alias("transaction_type")
        )

    val transformedDf =
      BankTransactionTransformer.transform(
        transactionsDf
      )

    val aggregatedDf =
      TransactionAggregation.aggregate(
        transformedDf
      )

    val query =
      aggregatedDf
        .writeStream
        .format("console")
        .outputMode("update")
        .option(
          "truncate",
          false
        )
        .option(
          "numRows",
          50
        )
        .option(
          "checkpointLocation",
          "/tmp/ucando/checkpoints/transaction-aggregation"
        )
        .start()

    query.awaitTermination()
  }
}