import model.BankTransactionSchema.schema
import model.CustomerBehaviorSchema
import model.LoanEventSchema
import model.InsuranceEventSchema
import model.CreditEventSchema
import model.MarketingEventSchema
import model.CustomerProfileSchema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MultiDomainStreamingJob {

  private val bootstrapServers =
    "localhost:9092"

  private val bronzePath =
    "/tmp/ucando/data/bronze/events"

  private val checkpointPath =
    "/tmp/ucando/checkpoints/bronze-events"

  def readTopic(
      spark: SparkSession,
      topic: String
  ): DataFrame = {

    spark.readStream
      .format("kafka")
      .option(
        "kafka.bootstrap.servers",
        bootstrapServers
      )
      .option(
        "subscribe",
        topic
      )
      .option(
        "startingOffsets",
        "latest"
      )
      .load()
  }

  def parseTopic(
      kafkaDf: DataFrame,
      schema: org.apache.spark.sql.types.StructType
  ): DataFrame = {

    kafkaDf
      .select(
        col("key")
          .cast("string")
          .alias("kafka_key"),

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
  }

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .appName("MultiDomainBronzeStreaming")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // BANK TRANSACTIONS
    // ============================================================

    val bankEvents =
      parseTopic(
        readTopic(spark, "bank.transactions"),
        schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("bank_transaction").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // CUSTOMER BEHAVIOR
    // ============================================================

    val behaviorEvents =
      parseTopic(
        readTopic(spark, "customer.behavior"),
        CustomerBehaviorSchema.schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("customer_behavior").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // LOAN
    // ============================================================

    val loanEvents =
      parseTopic(
        readTopic(spark, "loan.events"),
        LoanEventSchema.schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("loan").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // INSURANCE
    // ============================================================

    val insuranceEvents =
      parseTopic(
        readTopic(spark, "insurance.events"),
        InsuranceEventSchema.schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("insurance").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // CREDIT
    // ============================================================

    val creditEvents =
      parseTopic(
        readTopic(spark, "credit.events"),
        CreditEventSchema.schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("credit").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // MARKETING
    // ============================================================

    val marketingEvents =
      parseTopic(
        readTopic(spark, "marketing.events"),
        MarketingEventSchema.schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("marketing").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // CUSTOMER CDC
    // ============================================================

    val cdcEvents =
      parseTopic(
        readTopic(spark, "customer.cdc"),
        CustomerProfileSchema.schema
      )
      .select(
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("event.dataset").alias("dataset"),
        col("event.emitted_at").alias("emitted_at"),
        col("event.sequence").alias("sequence"),

        col("event.payload.event_id").alias("event_id"),
        col("event.payload.event_time").alias("event_time"),
        col("event.payload.customer_id").alias("customer_id"),

        lit("customer_cdc").alias("event_domain"),

        col("event_json").alias("raw_event")
      )

    // ============================================================
    // UNIFIED BRONZE STREAM
    // ============================================================

    val unifiedEvents =
      bankEvents
        .unionByName(behaviorEvents)
        .unionByName(loanEvents)
        .unionByName(insuranceEvents)
        .unionByName(creditEvents)
        .unionByName(marketingEvents)
        .unionByName(cdcEvents)

    // ============================================================
    // BRONZE METADATA
    // ============================================================

    val bronzeEvents =
      unifiedEvents
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
        .withColumn(
          "event_date",
          to_date(col("event_time"))
        )
        .withColumn(
          "ingested_at",
          current_timestamp()
        )

    // ============================================================
    // WRITE BRONZE
    // ============================================================

    val query =
      bronzeEvents
        .writeStream
        .format("parquet")
        .outputMode("append")
        .option(
          "path",
          bronzePath
        )
        .option(
          "checkpointLocation",
          checkpointPath
        )
        .partitionBy(
          "event_domain",
          "event_date"
        )
        .start()

    println(
      s"Bronze streaming started. Output: $bronzePath"
    )

    query.awaitTermination()
  }
}