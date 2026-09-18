package model

import org.apache.spark.sql.types._

object SilverCustomerCdcSchema {

  val schema: StructType =
    StructType(
      Seq(
        StructField("event_id", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("event_time", TimestampType, true),
        StructField("event_date", DateType, true),
        StructField("operation", StringType, true),
        StructField("changed_fields", StringType, true),
        StructField("source_system", StringType, true),
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