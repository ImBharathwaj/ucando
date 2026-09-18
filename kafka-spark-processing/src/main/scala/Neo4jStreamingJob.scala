import org.apache.spark.sql.{Dataset, Row, SparkSession}

import model.SilverTransactionSchema
import model.SilverBehaviorSchema
import model.SilverLoanSchema
import model.SilverInsuranceSchema
import model.SilverCreditSchema
import model.SilverMarketingSchema
import model.SilverCustomerCdcSchema

object Neo4jStreamingJob {

  private val silverBasePath =
    "/tmp/ucando/data/silver"

  private val checkpointBasePath =
    "/tmp/ucando/checkpoints/neo4j"

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .appName("UCanDo Neo4j Streaming")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println(
      """
        UCanDo Neo4j Streaming Layer Started
        transactions → Neo4j
        behavior     → Neo4j
        loans        → Neo4j
        insurance    → Neo4j
        credit       → Neo4j
        marketing    → Neo4j
        customer_cdc → Neo4j
        """.stripMargin
    )

    Neo4jSink.verifyConnection()

    Neo4jSink.initializeSchema()

    startTransactions(spark)

    startBehavior(spark)

    startLoans(spark)

    startInsurance(spark)

    startCredit(spark)

    startMarketing(spark)

    startCustomerCdc(spark)

    spark.streams.awaitAnyTermination()
  }

  // ============================================================
  // TRANSACTIONS
  // ============================================================

  private def startTransactions(
      spark: SparkSession
  ): Unit = {

    val transactions =
      spark.readStream
        .format("parquet")
        .schema(SilverTransactionSchema.schema)
        .load(
          s"$silverBasePath/transactions"
        )

    transactions.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Transactions")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )

          Neo4jSink.writeTransactions(batchDf)

          println(
            s"Neo4j transactions batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/transactions"
      )
      .start()
  }

  // ============================================================
  // BEHAVIOR
  // ============================================================

  private def startBehavior(
      spark: SparkSession
  ): Unit = {

    val behavior =
      spark.readStream
        .format("parquet")
        .schema(SilverBehaviorSchema.schema)
        .load(
          s"$silverBasePath/behavior"
        )

    behavior.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Behavior")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )

          Neo4jSink.writeBehavior(batchDf)

          println(
            s"Neo4j behavior batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/behavior"
      )
      .start()
  }

  // ============================================================
  // LOANS
  // ============================================================

  private def startLoans(
      spark: SparkSession
  ): Unit = {

    val loans =
      spark.readStream
        .format("parquet")
        .schema(SilverLoanSchema.schema)
        .load(
          s"$silverBasePath/loans"
        )

    loans.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Loans")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )

          Neo4jSink.writeLoans(batchDf)

          println(
            s"Neo4j loans batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/loans"
      )
      .start()
  }

  // ============================================================
  // INSURANCE
  // ============================================================

  private def startInsurance(
      spark: SparkSession
  ): Unit = {

    val insurance =
      spark.readStream
        .format("parquet")
        .schema(SilverInsuranceSchema.schema)
        .load(
          s"$silverBasePath/insurance"
        )

    insurance.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Insurance")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )

          Neo4jSink.writeInsurance(batchDf)

          println(
            s"Neo4j insurance batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/insurance"
      )
      .start()
  }

  // ============================================================
  // CREDIT
  // ============================================================

  private def startCredit(
      spark: SparkSession
  ): Unit = {

    val credit =
      spark.readStream
        .format("parquet")
        .schema(SilverCreditSchema.schema)
        .load(
          s"$silverBasePath/credit"
        )

    credit.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Credit")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )


          Neo4jSink.writeCredit(batchDf)

          println(
            s"Neo4j credit batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/credit"
      )
      .start()
  }

  // ============================================================
  // MARKETING
  // ============================================================

  private def startMarketing(
      spark: SparkSession
  ): Unit = {

    val marketing =
      spark.readStream
        .format("parquet")
        .schema(SilverMarketingSchema.schema)
        .load(
          s"$silverBasePath/marketing"
        )

    marketing.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Marketing")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )

          Neo4jSink.writeMarketing(batchDf)

          println(
            s"Neo4j marketing batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/marketing"
      )
      .start()
  }

  // ============================================================
  // CUSTOMER CDC
  // ============================================================

  private def startCustomerCdc(
      spark: SparkSession
  ): Unit = {

    val customerCdc =
      spark.readStream
        .format("parquet")
        .schema(SilverCustomerCdcSchema.schema)
        .load(
          s"$silverBasePath/customer_cdc"
        )

    customerCdc.writeStream
      .foreachBatch {
        (
            batchDf: Dataset[Row],
            batchId: Long
        ) =>

          println("Neo4j Customer CDC")

          println(s"Batch ID : $batchId")

          println(
            s"Records  : ${batchDf.count()}"
          )

          Neo4jSink.writeCustomerCdc(batchDf)

          println(
            s"Neo4j customer CDC batch $batchId completed"
          )
      }
      .option(
        "checkpointLocation",
        s"$checkpointBasePath/customer_cdc"
      )
      .start()
  }
}