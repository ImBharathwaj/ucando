package model

import org.apache.spark.sql.types._

object SilverTransactionSchema {

  val schema: StructType =
    StructType(
      Seq(
        StructField(
          "event_id",
          StringType,
          nullable = true
        ),

        StructField(
          "customer_id",
          StringType,
          nullable = true
        ),

        StructField(
          "account_id",
          StringType,
          nullable = true
        ),

        StructField(
          "transaction_id",
          StringType,
          nullable = true
        ),

        StructField(
          "event_time",
          TimestampType,
          nullable = true
        ),

        StructField(
          "event_date",
          DateType,
          nullable = true
        ),

        StructField(
          "transaction_type",
          StringType,
          nullable = true
        ),

        StructField(
          "amount",
          DoubleType,
          nullable = true
        ),

        StructField(
          "currency",
          StringType,
          nullable = true
        ),

        StructField(
          "merchant_id",
          StringType,
          nullable = true
        ),

        StructField(
          "merchant_name",
          StringType,
          nullable = true
        ),

        StructField(
          "merchant_category",
          StringType,
          nullable = true
        ),

        StructField(
          "channel",
          StringType,
          nullable = true
        ),

        StructField(
          "location",
          StringType,
          nullable = true
        ),

        StructField(
          "balance_after",
          DoubleType,
          nullable = true
        ),

        StructField(
          "customer_phone",
          StringType,
          nullable = true
        ),

        StructField(
          "kafka_topic",
          StringType,
          nullable = true
        ),

        StructField(
          "kafka_partition",
          IntegerType,
          nullable = true
        ),

        StructField(
          "kafka_offset",
          LongType,
          nullable = true
        ),

        StructField(
          "kafka_timestamp",
          TimestampType,
          nullable = true
        ),

        StructField(
          "sequence",
          LongType,
          nullable = true
        ),

        StructField(
          "emitted_at",
          TimestampType,
          nullable = true
        )
      )
    )
}