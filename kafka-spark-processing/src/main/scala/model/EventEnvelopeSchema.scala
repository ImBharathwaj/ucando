package model

import org.apache.spark.sql.types._

object EventEnvelopeSchema {

  val schema: StructType =
    StructType(
      Seq(
        StructField(
          "dataset",
          StringType,
          false
        ),

        StructField(
          "topic",
          StringType,
          false
        ),

        StructField(
          "emitted_at",
          StringType,
          false
        ),

        StructField(
          "sequence",
          LongType,
          false
        ),

        StructField(
          "payload",
          MapType(
            StringType,
            StringType,
            valueContainsNull = true
          ),
          true
        )
      )
    )
}