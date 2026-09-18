package model

import org.apache.spark.sql.types._

object MarketingEventSchema {

  val schema: StructType =
    StructType(
      Seq(
        StructField("dataset", StringType, false),
        StructField("topic", StringType, false),
        StructField("emitted_at", StringType, false),
        StructField("sequence", LongType, false),

        StructField(
          "payload",
          StructType(
            Seq(
              StructField("event_id", StringType, true),
              StructField("event_time", StringType, true),
              StructField("customer_id", StringType, true),
              StructField("campaign_id", StringType, true),
              StructField("event_type", StringType, true),
              StructField("channel", StringType, true),
              StructField("product", StringType, true),
              StructField("message_id", StringType, true),
              StructField("device_id", StringType, true),
              StructField("customer_phone", StringType, true)
            )
          ),
          true
        )
      )
    )
}