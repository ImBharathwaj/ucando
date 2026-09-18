package model

import org.apache.spark.sql.types._

object CustomerBehaviorSchema {

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
              StructField("session_id", StringType, true),
              StructField("device_id", StringType, true),
              StructField("channel", StringType, true),
              StructField("event_type", StringType, true),
              StructField("page", StringType, true),
              StructField("product", StringType, true),
              StructField("campaign_id", StringType, true),
              StructField("source", StringType, true),
              StructField("device_type", StringType, true),
              StructField("ip_country", StringType, true),
              StructField("customer_phone", StringType, true)
            )
          ),
          true
        )
      )
    )
}