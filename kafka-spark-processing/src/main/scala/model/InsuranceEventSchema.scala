package model

import org.apache.spark.sql.types._

object InsuranceEventSchema {

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
              StructField("policy_id", StringType, true),
              StructField("event_type", StringType, true),
              StructField("policy_type", StringType, true),
              StructField("premium_amount", DoubleType, true),
              StructField("channel", StringType, true),
              StructField("customer_phone", StringType, true)
            )
          ),
          true
        )
      )
    )
}