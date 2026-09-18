import org.apache.spark.sql.{DataFrame, Row}

import org.neo4j.driver.{
  AuthTokens,
  GraphDatabase,
  SessionConfig
}

import java.util

object Neo4jSink {

  // ============================================================
  // NEO4J CONFIGURATION
  // ============================================================

  private val uri =
    "bolt://localhost:7687"

  private val username =
    "neo4j"

  private val password =
    "strongroot"

  private val database =
    "neo4j"

  private val batchSize =
    500


  // ============================================================
  // NEO4J BATCH PARAMETERS
  // ============================================================

  private def batchParams(
      batch: util.List[util.Map[String, Object]]
  ): util.Map[String, Object] = {

    val params =
      new util.HashMap[String, Object]()

    params.put(
      "rows",
      batch
    )

    params
  }


  // ============================================================
  // CONNECTION TEST
  // ============================================================

  def verifyConnection(): Unit = {

    val driver =
      GraphDatabase.driver(
        uri,
        AuthTokens.basic(
          username,
          password
        )
      )

    try {

      driver.verifyConnectivity()

      println(
        "Neo4j connectivity verified"
      )

    } finally {

      driver.close()
    }
  }


  // ============================================================
  // NEO4J CONSTRAINTS
  // ============================================================

  def initializeSchema(): Unit = {

    val driver =
      GraphDatabase.driver(
        uri,
        AuthTokens.basic(
          username,
          password
        )
      )

    val session =
      driver.session(
        SessionConfig
          .builder()
          .withDatabase(database)
          .build()
      )

    try {

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
          FOR (c:Customer)
          REQUIRE c.customer_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT account_id_unique IF NOT EXISTS
          FOR (a:Account)
          REQUIRE a.account_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT transaction_id_unique IF NOT EXISTS
          FOR (t:Transaction)
          REQUIRE t.transaction_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT merchant_id_unique IF NOT EXISTS
          FOR (m:Merchant)
          REQUIRE m.merchant_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT behavior_event_id_unique IF NOT EXISTS
          FOR (b:BehaviorEvent)
          REQUIRE b.event_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT device_id_unique IF NOT EXISTS
          FOR (d:Device)
          REQUIRE d.device_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT loan_id_unique IF NOT EXISTS
          FOR (l:Loan)
          REQUIRE l.loan_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT insurance_policy_id_unique IF NOT EXISTS
          FOR (p:InsurancePolicy)
          REQUIRE p.policy_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT credit_event_id_unique IF NOT EXISTS
          FOR (ce:CreditEvent)
          REQUIRE ce.event_id IS UNIQUE
          """
        ).consume()


        tx.run(
          """
          CREATE CONSTRAINT campaign_id_unique IF NOT EXISTS
          FOR (ca:Campaign)
          REQUIRE ca.campaign_id IS UNIQUE
          """
        ).consume()
      }

      println(
        "Neo4j constraints initialized"
      )

    } finally {

      session.close()
      driver.close()
    }
  }


  // ============================================================
  // TRANSACTIONS
  // ============================================================

  def writeTransactions(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "transactions"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          MERGE (t:Transaction {
            transaction_id: row.transaction_id
          })

          SET
            t.event_id = row.event_id,
            t.amount = row.amount,
            t.currency = row.currency,
            t.transaction_type = row.transaction_type,
            t.channel = row.channel,
            t.event_time = row.event_time

          MERGE (c)-[:MADE]->(t)

          FOREACH (_ IN CASE
            WHEN row.account_id IS NOT NULL
            THEN [1]
            ELSE []
          END |
            MERGE (a:Account {
              account_id: row.account_id
            })

            MERGE (c)-[:OWNS]->(a)
          )

          FOREACH (_ IN CASE
            WHEN row.merchant_id IS NOT NULL
            THEN [1]
            ELSE []
          END |
            MERGE (m:Merchant {
              merchant_id: row.merchant_id
            })

            SET
              m.name = row.merchant_name,
              m.category = row.merchant_category

            MERGE (t)-[:AT]->(m)
          )
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // CUSTOMER BEHAVIOR
  // ============================================================

  def writeBehavior(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "behavior"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          MERGE (b:BehaviorEvent {
            event_id: row.event_id
          })

          SET
            b.event_type = row.event_type,
            b.channel = row.channel,
            b.page = row.page,
            b.product = row.product,
            b.session_id = row.session_id,
            b.event_time = row.event_time

          MERGE (c)-[:PERFORMED]->(b)

          FOREACH (_ IN CASE
            WHEN row.device_id IS NOT NULL
            THEN [1]
            ELSE []
          END |

            MERGE (d:Device {
              device_id: row.device_id
            })

            MERGE (b)-[:USING]->(d)

            MERGE (c)-[:USED]->(d)
          )
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // LOANS
  // ============================================================

  def writeLoans(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "loans"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          FOREACH (_ IN CASE
            WHEN row.loan_id IS NOT NULL
            THEN [1]
            ELSE []
          END |

            MERGE (l:Loan {
              loan_id: row.loan_id
            })

            SET
              l.application_id = row.application_id,
              l.loan_type = row.loan_type,
              l.amount = row.amount,
              l.status = row.status,
              l.channel = row.channel,
              l.last_event_type = row.event_type,
              l.event_time = row.event_time

            MERGE (c)-[:APPLIED_FOR]->(l)
          )
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // INSURANCE
  // ============================================================

  def writeInsurance(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "insurance"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          FOREACH (_ IN CASE
            WHEN row.policy_id IS NOT NULL
            THEN [1]
            ELSE []
          END |

            MERGE (p:InsurancePolicy {
              policy_id: row.policy_id
            })

            SET
              p.policy_type = row.policy_type,
              p.premium_amount = row.premium_amount,
              p.channel = row.channel,
              p.last_event_type = row.event_type,
              p.event_time = row.event_time

            MERGE (c)-[:HAS_POLICY]->(p)
          )
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // CREDIT
  // ============================================================

  def writeCredit(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "credit"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          MERGE (ce:CreditEvent {
            event_id: row.event_id
          })

          SET
            ce.bureau = row.bureau,
            ce.event_type = row.event_type,
            ce.credit_score = row.credit_score,
            ce.score_change = row.score_change,
            ce.enquiry_count = row.enquiry_count,
            ce.event_time = row.event_time

          MERGE (c)-[:HAS_CREDIT_EVENT]->(ce)
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // MARKETING
  // ============================================================

  def writeMarketing(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "marketing"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          FOREACH (_ IN CASE
            WHEN row.campaign_id IS NOT NULL
            THEN [1]
            ELSE []
          END |

            MERGE (ca:Campaign {
              campaign_id: row.campaign_id
            })

            SET
              ca.channel = row.channel,
              ca.product = row.product

            MERGE (c)-[r:INTERACTED_WITH]->(ca)

            SET
              r.event_type = row.event_type,
              r.message_id = row.message_id,
              r.event_time = row.event_time
          )
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // CUSTOMER CDC
  // ============================================================

  def writeCustomerCdc(
      df: DataFrame
  ): Unit = {

    writeInPartitions(
      df,
      "customer_cdc"
    ) { (session, batch) =>

      session.executeWriteWithoutResult { tx =>

        tx.run(
          """
          UNWIND $rows AS row

          MERGE (c:Customer {
            customer_id: row.customer_id
          })

          SET
            c.last_cdc_operation = row.operation,
            c.last_cdc_source = row.source_system,
            c.last_cdc_event_time = row.event_time
          """,
          batchParams(batch)
        ).consume()
      }
    }
  }


  // ============================================================
  // GENERIC PARTITION WRITER
  // ============================================================
  //
  // IMPORTANT:
  //
  // We intentionally coalesce to ONE Spark partition before
  // sending a micro-batch to Neo4j.
  //
  // Why?
  //
  // Multiple Spark partitions can simultaneously modify the
  // same Customer / Account / Device / Merchant nodes.
  //
  // That was producing Neo4j Forseti lock contention:
  //
  // NODE_RELATIONSHIP_GROUP_DELETE
  //
  // For this local showcase workload, one partition gives us
  // deterministic Neo4j writes and avoids unnecessary
  // concurrent graph mutations.
  //
  // ============================================================

  private def writeInPartitions(
      df: DataFrame,
      domain: String
  )(
      writeBatch:
        (
          org.neo4j.driver.Session,
          util.List[util.Map[String, Object]]
        ) => Unit
  ): Unit = {

    if (df.isEmpty) {
      return
    }

    /*
     * Remove rows that do not contain the mandatory identity
     * for the graph operation.
     *
     * Optional entity IDs are handled inside Cypher.
     */
    val cleanedDf =
      domain match {

        case "transactions" =>
          df.filter(
            "customer_id IS NOT NULL AND transaction_id IS NOT NULL"
          )

        case "behavior" =>
          df.filter(
            "customer_id IS NOT NULL AND event_id IS NOT NULL"
          )

        case "loans" =>
          df.filter(
            "customer_id IS NOT NULL"
          )

        case "insurance" =>
          df.filter(
            "customer_id IS NOT NULL"
          )

        case "credit" =>
          df.filter(
            "customer_id IS NOT NULL AND event_id IS NOT NULL"
          )

        case "marketing" =>
          df.filter(
            "customer_id IS NOT NULL"
          )

        case "customer_cdc" =>
          df.filter(
            "customer_id IS NOT NULL"
          )

        case _ =>
          throw new IllegalArgumentException(
            s"Unknown Neo4j domain: $domain"
          )
      }

    if (cleanedDf.isEmpty) {
      println(
        s"[$domain] No valid rows after mandatory ID validation"
      )
      return
    }

    /*
     * IMPORTANT:
     *
     * This deliberately serializes the Spark side of each
     * micro-batch to one task.
     *
     * We are running locally and Neo4j is our graph sink.
     * The priority here is correctness and deterministic
     * graph mutation rather than maximum write parallelism.
     */
    cleanedDf
      .coalesce(1)
      .foreachPartition {

        partition: Iterator[Row] =>

          val driver =
            GraphDatabase.driver(
              uri,
              AuthTokens.basic(
                username,
                password
              )
            )

          val session =
            driver.session(
              SessionConfig
                .builder()
                .withDatabase(database)
                .build()
            )

          try {

            var batch =
              new util.ArrayList[
                util.Map[String, Object]
              ]()

            partition.foreach { row =>

              batch.add(
                rowToMap(
                  row,
                  domain
                )
              )

              if (batch.size() >= batchSize) {

                writeBatch(
                  session,
                  batch
                )

                batch =
                  new util.ArrayList[
                    util.Map[String, Object]
                  ]()
              }
            }

            if (!batch.isEmpty) {

              writeBatch(
                session,
                batch
              )
            }

          } finally {

            session.close()
            driver.close()
          }
      }
  }


  // ============================================================
  // ROW → NEO4J PARAMETER MAP
  // ============================================================

  private def rowToMap(
      row: Row,
      domain: String
  ): util.Map[String, Object] = {

    val map =
      new util.HashMap[String, Object]()


    // ----------------------------------------------------------
    // STRING
    // ----------------------------------------------------------

    def putString(
        column: String
    ): Unit = {

      val index =
        row.fieldIndex(column)

      if (!row.isNullAt(index)) {

        val value =
          row.getAs[String](column)

        if (value != null) {

          map.put(
            column,
            value
          )
        }
      }
    }


    // ----------------------------------------------------------
    // DOUBLE
    // ----------------------------------------------------------

    def putDouble(
        column: String
    ): Unit = {

      val index =
        row.fieldIndex(column)

      if (!row.isNullAt(index)) {

        val value =
          row.getAs[java.lang.Double](column)

        if (value != null) {

          map.put(
            column,
            value
          )
        }
      }
    }


    // ----------------------------------------------------------
    // INTEGER
    // ----------------------------------------------------------

    def putInt(
        column: String
    ): Unit = {

      val index =
        row.fieldIndex(column)

      if (!row.isNullAt(index)) {

        val value =
          row.getAs[java.lang.Integer](column)

        if (value != null) {

          map.put(
            column,
            value
          )
        }
      }
    }


    // ----------------------------------------------------------
    // LONG
    // ----------------------------------------------------------

    def putLong(
        column: String
    ): Unit = {

      val index =
        row.fieldIndex(column)

      if (!row.isNullAt(index)) {

        val value =
          row.getAs[java.lang.Long](column)

        if (value != null) {

          map.put(
            column,
            value
          )
        }
      }
    }


    // ----------------------------------------------------------
    // TIMESTAMP
    // ----------------------------------------------------------

    def putTimestamp(
        column: String
    ): Unit = {

      val index =
        row.fieldIndex(column)

      if (!row.isNullAt(index)) {

        val timestamp =
          row.getAs[java.sql.Timestamp](column)

        if (timestamp != null) {

          map.put(
            column,
            timestamp
              .toInstant
              .toString
          )
        }
      }
    }


    // ==========================================================
    // DOMAIN MAPPING
    // ==========================================================

    domain match {


      // ========================================================
      // TRANSACTIONS
      // ========================================================

      case "transactions" =>

        putString("customer_id")
        putString("account_id")
        putString("transaction_id")

        putString("merchant_id")
        putString("merchant_name")
        putString("merchant_category")

        putDouble("amount")

        putString("currency")
        putString("transaction_type")
        putString("channel")

        putString("event_id")

        putTimestamp("event_time")


      // ========================================================
      // BEHAVIOR
      // ========================================================

      case "behavior" =>

        putString("customer_id")
        putString("device_id")
        putString("event_id")

        putString("event_type")
        putString("channel")
        putString("page")
        putString("product")
        putString("session_id")

        putTimestamp("event_time")


      // ========================================================
      // LOANS
      // ========================================================

      case "loans" =>

        putString("customer_id")
        putString("loan_id")
        putString("application_id")

        putString("loan_type")

        putDouble("amount")

        putString("status")
        putString("channel")
        putString("event_type")

        putTimestamp("event_time")


      // ========================================================
      // INSURANCE
      // ========================================================

      case "insurance" =>

        putString("customer_id")
        putString("policy_id")

        putString("policy_type")

        putDouble("premium_amount")

        putString("channel")
        putString("event_type")

        putTimestamp("event_time")


      // ========================================================
      // CREDIT
      // ========================================================

      case "credit" =>

        putString("customer_id")
        putString("event_id")

        putString("bureau")
        putString("event_type")

        putInt("credit_score")
        putInt("score_change")
        putInt("enquiry_count")

        putTimestamp("event_time")


      // ========================================================
      // MARKETING
      // ========================================================

      case "marketing" =>

        putString("customer_id")
        putString("campaign_id")

        putString("event_type")
        putString("channel")
        putString("product")
        putString("message_id")

        putTimestamp("event_time")


      // ========================================================
      // CUSTOMER CDC
      // ========================================================

      case "customer_cdc" =>

        putString("customer_id")
        putString("event_id")

        putString("operation")
        putString("source_system")

        putTimestamp("event_time")


      // ========================================================
      // UNKNOWN
      // ========================================================

      case _ =>

        throw new IllegalArgumentException(
          s"Unknown Neo4j domain: $domain"
        )
    }

    map
  }
}