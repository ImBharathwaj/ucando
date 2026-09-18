import org.apache.spark.sql.SparkSession

object ReadBronze{

    def main(args: Array[String]): Unit = {
        val spark = SparkSession
            .builder()
            .appName("ReadBronze")
            .master("local[*]")
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")

        val bronze = spark.read.parquet("/tmp/ucando/data/bronze/events")

        println("BRONZE SCHEMA")
        bronze.printSchema()

        println("BRONZE SAMPLE")
        bronze
            .orderBy("event_domain", "event_time")
            .show(30, truncate=false)
        
        println("EVENTS BY DOMAIN")

        bronze
            .groupBy("event_domain")
            .count()
            .orderBy("event_domain")
            .show(false)

        spark.stop()
    }
}