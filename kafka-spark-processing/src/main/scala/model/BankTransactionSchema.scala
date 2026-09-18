package model

import org.apache.spark.sql.types._

object BankTransactionSchema {

  val schema: StructType =
    StructType(
      Seq(
        StructField(
          "dataset",
          StringType,
          nullable = false
        ),

        StructField(
          "emitted_at",
          StringType,
          nullable = false
        ),

        StructField(
          "sequence",
          LongType,
          nullable = false
        ),

        StructField(
          "topic",
          StringType,
          nullable = false
        ),

        StructField(
          "payload",
          StructType(
            Seq(
              StructField("account_id", StringType, true),
              StructField("amount", DoubleType, true),
              StructField("balance_after", DoubleType, true),
              StructField("channel", StringType, true),
              StructField("currency", StringType, true),
              StructField("customer_id", StringType, true),
              StructField("customer_phone", StringType, true),
              StructField("event_id", StringType, true),
              StructField("event_time", StringType, true),
              StructField("location", StringType, true),
              StructField("merchant_category", StringType, true),
              StructField("merchant_id", StringType, true),
              StructField("merchant_name", StringType, true),
              StructField("transaction_id", StringType, true),
              StructField("transaction_type", StringType, true)
            )
          ),
          nullable = false
        )
      )
    )
}