package model

import org.apache.spark.sql.types._

object LoanEventSchema {

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
              StructField("loan_id", StringType, true),
              StructField("application_id", StringType, true),
              StructField("event_type", StringType, true),
              StructField("loan_type", StringType, true),
              StructField("amount", DoubleType, true),
              StructField("status", StringType, true),
              StructField("channel", StringType, true),
              StructField("customer_phone", StringType, true)
            )
          ),
          true
        )
      )
    )
}