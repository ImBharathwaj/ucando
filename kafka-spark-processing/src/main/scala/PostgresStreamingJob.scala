import org.apache.spark.sql.{Dataset, Row, SparkSession}

object PostgresStreamingJob {

  private val silverBasePath =
    "/tmp/ucando/data/silver"

  private val checkpointBasePath =
    "/tmp/ucando/checkpoints/postgres"

  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession.builder()
        .appName("PostgresStreamingJob")
        .master("local[*]")
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // ------------------------------------------------------------------------
    // IMPORTANT
    //
    // Spark cannot infer the schema for a streaming Parquet source.
    //
    // Therefore:
    //
    // 1. Read each Silver directory once as a STATIC DataFrame.
    // 2. Extract its schema.
    // 3. Reuse that schema for readStream.
    //
    // ------------------------------------------------------------------------

    val transactionsSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/transactions")
        .schema

    val behaviorSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/behavior")
        .schema

    val loansSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/loans")
        .schema

    val insuranceSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/insurance")
        .schema

    val creditSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/credit")
        .schema

    val marketingSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/marketing")
        .schema

    val customerCdcSchema =
      spark.read
        .format("parquet")
        .load(s"$silverBasePath/customer_cdc")
        .schema

    // ------------------------------------------------------------------------
    // SILVER STREAMS
    // ------------------------------------------------------------------------

    val transactions =
      spark.readStream
        .schema(transactionsSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/transactions"
        )

    val behavior =
      spark.readStream
        .schema(behaviorSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/behavior"
        )

    val loans =
      spark.readStream
        .schema(loansSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/loans"
        )

    val insurance =
      spark.readStream
        .schema(insuranceSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/insurance"
        )

    val credit =
      spark.readStream
        .schema(creditSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/credit"
        )

    val marketing =
      spark.readStream
        .schema(marketingSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/marketing"
        )

    val customerCdc =
      spark.readStream
        .schema(customerCdcSchema)
        .format("parquet")
        .load(
          s"$silverBasePath/customer_cdc"
        )

    // ------------------------------------------------------------------------
    // TRANSACTIONS → POSTGRES
    // ------------------------------------------------------------------------

    val transactionsQuery =
      transactions.writeStream
        .queryName("postgres-transactions")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES TRANSACTIONS
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeTransactions(batchDf)

            println(
              s"Transactions batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/transactions"
        )
        .start()

    // ------------------------------------------------------------------------
    // BEHAVIOR → POSTGRES
    // ------------------------------------------------------------------------

    val behaviorQuery =
      behavior.writeStream
        .queryName("postgres-behavior")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES BEHAVIOR
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeBehavior(batchDf)

            println(
              s"Behavior batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/behavior"
        )
        .start()

    // ------------------------------------------------------------------------
    // LOANS → POSTGRES
    // ------------------------------------------------------------------------

    val loansQuery =
      loans.writeStream
        .queryName("postgres-loans")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES LOANS
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeLoans(batchDf)

            println(
              s"Loans batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/loans"
        )
        .start()

    // ------------------------------------------------------------------------
    // INSURANCE → POSTGRES
    // ------------------------------------------------------------------------

    val insuranceQuery =
      insurance.writeStream
        .queryName("postgres-insurance")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES INSURANCE
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeInsurance(batchDf)

            println(
              s"Insurance batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/insurance"
        )
        .start()

    // ------------------------------------------------------------------------
    // CREDIT → POSTGRES
    // ------------------------------------------------------------------------

    val creditQuery =
      credit.writeStream
        .queryName("postgres-credit")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES CREDIT
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeCredit(batchDf)

            println(
              s"Credit batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/credit"
        )
        .start()

    // ------------------------------------------------------------------------
    // MARKETING → POSTGRES
    // ------------------------------------------------------------------------

    val marketingQuery =
      marketing.writeStream
        .queryName("postgres-marketing")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES MARKETING
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeMarketing(batchDf)

            println(
              s"Marketing batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/marketing"
        )
        .start()

    // ------------------------------------------------------------------------
    // CUSTOMER CDC → POSTGRES
    // ------------------------------------------------------------------------

    val customerCdcQuery =
      customerCdc.writeStream
        .queryName("postgres-customer-cdc")
        .foreachBatch {
          (batchDf: Dataset[Row], batchId: Long) =>

            println(
              s"""
                 |============================================================
                 |POSTGRES CUSTOMER CDC
                 |Batch ID : $batchId
                 |Records  : ${batchDf.count()}
                 |============================================================
                 |""".stripMargin
            )

            PostgresSink.writeCustomerCdc(batchDf)

            println(
              s"Customer CDC batch $batchId completed"
            )
        }
        .option(
          "checkpointLocation",
          s"$checkpointBasePath/customer-cdc"
        )
        .start()

    // ------------------------------------------------------------------------
    // STARTUP MESSAGE
    // ------------------------------------------------------------------------

    println(
      """
        |
        |======================================================================
        | PostgreSQL Streaming Layer Started
        |======================================================================
        |
        | transactions → PostgreSQL
        | behavior     → PostgreSQL
        | loans        → PostgreSQL
        | insurance    → PostgreSQL
        | credit       → PostgreSQL
        | marketing    → PostgreSQL
        | customer_cdc → PostgreSQL
        |
        |======================================================================
        |""".stripMargin
    )

    // ------------------------------------------------------------------------
    // WAIT
    // ------------------------------------------------------------------------

    spark.streams.awaitAnyTermination()
  }
}