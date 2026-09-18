import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SilverBatchJob {

  private val bronzePath =
    "/tmp/ucando/data/bronze/events"

  private val silverPath =
    "/tmp/ucando/data/silver"

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .appName("SilverBatchJob")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ============================================================
    // READ BRONZE
    // ============================================================

    val bronze =
      spark.read
        .parquet(bronzePath)

    println("========================================")
    println("BRONZE RECORD COUNT")
    println("========================================")

    println(bronze.count())

    // ============================================================
    // COMMON CLEANUP
    // ============================================================

    val cleaned =
      bronze
        .filter(col("event_id").isNotNull)
        .filter(col("customer_id").isNotNull)
        .filter(col("event_time").isNotNull)
        .dropDuplicates("event_id")

    println()
    println("========================================")
    println("CLEANED RECORD COUNT")
    println("========================================")

    println(cleaned.count())

    // ============================================================
    // NORMALIZE SEVEN DOMAINS
    // ============================================================

    val transactions =
      SilverDomainNormalizer
        .transactions(cleaned)

    val behavior =
      SilverDomainNormalizer
        .behavior(cleaned)

    val loans =
      SilverDomainNormalizer
        .loans(cleaned)

    val insurance =
      SilverDomainNormalizer
        .insurance(cleaned)

    val credit =
      SilverDomainNormalizer
        .credit(cleaned)

    val marketing =
      SilverDomainNormalizer
        .marketing(cleaned)

    val customerCdc =
      SilverDomainNormalizer
        .customerCdc(cleaned)

    // ============================================================
    // WRITE SILVER DATASETS
    // ============================================================

    transactions.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/transactions"
      )

    behavior.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/behavior"
      )

    loans.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/loans"
      )

    insurance.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/insurance"
      )

    credit.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/credit"
      )

    marketing.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/marketing"
      )

    customerCdc.write
      .mode("overwrite")
      .partitionBy("event_date")
      .parquet(
        s"$silverPath/customer_cdc"
      )

    // ============================================================
    // COUNTS
    // ============================================================

    println()
    println("========================================")
    println("SILVER DATASET COUNTS")
    println("========================================")

    println(
      s"transactions = ${transactions.count()}"
    )

    println(
      s"behavior     = ${behavior.count()}"
    )

    println(
      s"loans        = ${loans.count()}"
    )

    println(
      s"insurance    = ${insurance.count()}"
    )

    println(
      s"credit       = ${credit.count()}"
    )

    println(
      s"marketing    = ${marketing.count()}"
    )

    println(
      s"customer_cdc = ${customerCdc.count()}"
    )

    println()
    println("========================================")
    println("SILVER SCHEMAS")
    println("========================================")

    println()
    println("----- TRANSACTIONS -----")
    transactions.printSchema()

    println()
    println("----- BEHAVIOR -----")
    behavior.printSchema()

    println()
    println("----- LOANS -----")
    loans.printSchema()

    println()
    println("----- INSURANCE -----")
    insurance.printSchema()

    println()
    println("----- CREDIT -----")
    credit.printSchema()

    println()
    println("----- MARKETING -----")
    marketing.printSchema()

    println()
    println("----- CUSTOMER CDC -----")
    customerCdc.printSchema()

    spark.stop()
  }
}