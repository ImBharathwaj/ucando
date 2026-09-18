package model

import org.apache.spark.sql.types._

object CustomerProfileSchema {

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
              StructField("operation", StringType, true),

              StructField(
                "changed_fields",
                StructType(
                  Seq(
                    StructField("field", StringType, true),
                    StructField("old_value", StringType, true),
                    StructField("new_value", StringType, true)
                  )
                ),
                true
              ),

              StructField("source_system", StringType, true),
              StructField("customer_phone", StringType, true)
            )
          ),
          true
        )
      )
    )
}