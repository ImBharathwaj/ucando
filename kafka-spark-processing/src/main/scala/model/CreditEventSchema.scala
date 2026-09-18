package model

import org.apache.spark.sql.types._

object CreditEventSchema {

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
              StructField("bureau", StringType, true),
              StructField("event_type", StringType, true),
              StructField("credit_score", IntegerType, true),
              StructField("score_change", IntegerType, true),
              StructField("enquiry_count", IntegerType, true),
              StructField("customer_phone", StringType, true)
            )
          ),
          true
        )
      )
    )
}