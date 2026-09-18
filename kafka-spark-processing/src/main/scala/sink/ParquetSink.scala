package sink

import org.apache.spark.sql.DataFrame

object ParquetSink{
    def write(
        df: DataFrame,
        outputPath: String,
        checkpointPath: String
    ): Unit = {
        val query = df
            .writeStream
            .format("parquet")
            .outputMode("append")
            .option("path", outputPath)
            .option("checkpointLocation", checkpointPath)
            .partitionBy("event_date")
            .start()
            .awaitTermination()
    }
}