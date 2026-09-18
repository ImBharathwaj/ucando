package model

import org.apache.spark.sql.types._

object SilverBehaviorSchema {

  val schema: StructType =
    StructType(
      Seq(
        StructField("event_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("session_id", StringType, true),
        StructField("device_id", StringType, true),
        StructField("channel", StringType, true),
        StructField("event_type", StringType, true),
        StructField("page", StringType, true),
        StructField("product", StringType, true),
        StructField("campaign_id", StringType, true),
        StructField("source", StringType, true),
        StructField("metadata", StringType, true),

        StructField("event_time", TimestampType, true),
        StructField("event_date", DateType, true),

        StructField("customer_phone", StringType, true),

        StructField("kafka_topic", StringType, true),
        StructField("kafka_partition", IntegerType, true),
        StructField("kafka_offset", LongType, true),
        StructField("kafka_timestamp", TimestampType, true),

        StructField("sequence", LongType, true),
        StructField("emitted_at", TimestampType, true)
      )
    )
}