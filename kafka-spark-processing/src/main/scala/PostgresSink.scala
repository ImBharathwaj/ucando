import org.apache.spark.sql.{DataFrame, Row}

import java.sql.{
  Connection,
  Date,
  DriverManager,
  PreparedStatement,
  Timestamp
}

object PostgresSink {

  private val jdbcUrl = "jdbc:postgresql://localhost:5432/ucando"
  private val username = "postgres"
  private val password = "root"

  private val batchSize = 500

  Class.forName("org.postgresql.Driver")

  // ============================================================
  // CONNECTION
  // ============================================================

  private def withConnection[T](f: Connection => T): T = {

    val connection =
      DriverManager.getConnection(
        jdbcUrl,
        username,
        password
      )

    try {
      f(connection)
    } finally {
      connection.close()
    }
  }

  // ============================================================
  // NULL-SAFE JDBC HELPERS
  // ============================================================

  private def setString(
      statement: PreparedStatement,
      index: Int,
      row: Row,
      column: String
  ): Unit = {

    if (row.isNullAt(row.fieldIndex(column))) {
      statement.setNull(index, java.sql.Types.VARCHAR)
    } else {
      statement.setString(index, row.getAs[String](column))
    }
  }

  private def setDouble(
      statement: PreparedStatement,
      index: Int,
      row: Row,
      column: String
  ): Unit = {

    if (row.isNullAt(row.fieldIndex(column))) {
      statement.setNull(index, java.sql.Types.DOUBLE)
    } else {
      statement.setDouble(index, row.getAs[Double](column))
    }
  }

  private def setInt(
      statement: PreparedStatement,
      index: Int,
      row: Row,
      column: String
  ): Unit = {

    if (row.isNullAt(row.fieldIndex(column))) {
      statement.setNull(index, java.sql.Types.INTEGER)
    } else {
      statement.setInt(index, row.getAs[Int](column))
    }
  }

  private def setLong(
      statement: PreparedStatement,
      index: Int,
      row: Row,
      column: String
  ): Unit = {

    if (row.isNullAt(row.fieldIndex(column))) {
      statement.setNull(index, java.sql.Types.BIGINT)
    } else {
      statement.setLong(index, row.getAs[Long](column))
    }
  }

  private def setTimestamp(
      statement: PreparedStatement,
      index: Int,
      row: Row,
      column: String
  ): Unit = {

    if (row.isNullAt(row.fieldIndex(column))) {
      statement.setNull(index, java.sql.Types.TIMESTAMP)
    } else {
      statement.setTimestamp(
        index,
        row.getAs[Timestamp](column)
      )
    }
  }

  private def setDate(
      statement: PreparedStatement,
      index: Int,
      row: Row,
      column: String
  ): Unit = {

    if (row.isNullAt(row.fieldIndex(column))) {
      statement.setNull(index, java.sql.Types.DATE)
    } else {
      statement.setDate(
        index,
        row.getAs[Date](column)
      )
    }
  }

  // ============================================================
  // GENERIC BATCH EXECUTION
  // ============================================================

  private def executeBatch(
      statement: PreparedStatement,
      connection: Connection,
      counter: Int
  ): Unit = {

    if (counter > 0) {
      statement.executeBatch()
      connection.commit()
    }
  }

  // ============================================================
  // TRANSACTIONS
  // ============================================================

  def writeTransactions(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

        connection.setAutoCommit(false)

        val sql =
            """
                INSERT INTO transactions (
                event_id,
                customer_id,
                account_id,
                transaction_id,
                event_time,
                transaction_type,
                amount,
                currency,
                merchant_id,
                merchant_name,
                merchant_category,
                channel,
                location,
                balance_after,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at,
                event_date
                )
                VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT (event_id)
                DO NOTHING
            """
          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")
              setString(statement, 3, row, "account_id")
              setString(statement, 4, row, "transaction_id")

              setTimestamp(statement, 5, row, "event_time")

              setString(
                statement,
                6,
                row,
                "transaction_type"
              )

              setDouble(statement, 7, row, "amount")

              setString(statement, 8, row, "currency")
              setString(statement, 9, row, "merchant_id")
              setString(statement, 10, row, "merchant_name")
              setString(statement, 11, row, "merchant_category")
              setString(statement, 12, row, "channel")
              setString(statement, 13, row, "location")

              setDouble(
                statement,
                14,
                row,
                "balance_after"
              )

              setString(
                statement,
                15,
                row,
                "customer_phone"
              )

              setString(
                statement,
                16,
                row,
                "kafka_topic"
              )

              setInt(
                statement,
                17,
                row,
                "kafka_partition"
              )

              setLong(
                statement,
                18,
                row,
                "kafka_offset"
              )

              setTimestamp(
                statement,
                19,
                row,
                "kafka_timestamp"
              )

              setLong(
                statement,
                20,
                row,
                "sequence"
              )

              setTimestamp(
                statement,
                21,
                row,
                "emitted_at"
              )

              setDate(
                statement,
                22,
                row,
                "event_date"
              )

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }

  // ============================================================
  // CUSTOMER BEHAVIOR
  // ============================================================

  def writeBehavior(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

          connection.setAutoCommit(false)

          val sql =
            """
              INSERT INTO behavior_events (
                event_id,
                customer_id,
                session_id,
                device_id,
                event_time,
                event_date,
                channel,
                event_type,
                page,
                product,
                campaign_id,
                source,
                device_type,
                ip_country,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at
              )
              VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?
              )
              ON CONFLICT (event_id)
              DO NOTHING
            """

          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")
              setString(statement, 3, row, "session_id")
              setString(statement, 4, row, "device_id")

              setTimestamp(statement, 5, row, "event_time")
              setDate(statement, 6, row, "event_date")

              setString(statement, 7, row, "channel")
              setString(statement, 8, row, "event_type")
              setString(statement, 9, row, "page")
              setString(statement, 10, row, "product")
              setString(statement, 11, row, "campaign_id")
              setString(statement, 12, row, "source")
              setString(statement, 13, row, "device_type")
              setString(statement, 14, row, "ip_country")
              setString(statement, 15, row, "customer_phone")

              setString(statement, 16, row, "kafka_topic")
              setInt(statement, 17, row, "kafka_partition")
              setLong(statement, 18, row, "kafka_offset")
              setTimestamp(statement, 19, row, "kafka_timestamp")
              setLong(statement, 20, row, "sequence")
              setTimestamp(statement, 21, row, "emitted_at")

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }

  // ============================================================
  // LOANS
  // ============================================================

  def writeLoans(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

          connection.setAutoCommit(false)

          val sql =
            """
              INSERT INTO loans (
                event_id,
                customer_id,
                loan_id,
                application_id,
                event_time,
                event_date,
                event_type,
                loan_type,
                amount,
                status,
                channel,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at
              )
              VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
              )
              ON CONFLICT (event_id)
              DO NOTHING
            """

          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")
              setString(statement, 3, row, "loan_id")
              setString(statement, 4, row, "application_id")

              setTimestamp(statement, 5, row, "event_time")
              setDate(statement, 6, row, "event_date")

              setString(statement, 7, row, "event_type")
              setString(statement, 8, row, "loan_type")
              setDouble(statement, 9, row, "amount")
              setString(statement, 10, row, "status")
              setString(statement, 11, row, "channel")
              setString(statement, 12, row, "customer_phone")

              setString(statement, 13, row, "kafka_topic")
              setInt(statement, 14, row, "kafka_partition")
              setLong(statement, 15, row, "kafka_offset")
              setTimestamp(statement, 16, row, "kafka_timestamp")
              setLong(statement, 17, row, "sequence")
              setTimestamp(statement, 18, row, "emitted_at")

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }

  // ============================================================
  // INSURANCE
  // ============================================================

  def writeInsurance(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

          connection.setAutoCommit(false)

          val sql =
            """
              INSERT INTO insurance_policies (
                event_id,
                customer_id,
                policy_id,
                event_time,
                event_date,
                event_type,
                policy_type,
                premium_amount,
                channel,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at
              )
              VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
              )
              ON CONFLICT (event_id)
              DO NOTHING
            """

          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")
              setString(statement, 3, row, "policy_id")

              setTimestamp(statement, 4, row, "event_time")
              setDate(statement, 5, row, "event_date")

              setString(statement, 6, row, "event_type")
              setString(statement, 7, row, "policy_type")
              setDouble(statement, 8, row, "premium_amount")
              setString(statement, 9, row, "channel")
              setString(statement, 10, row, "customer_phone")

              setString(statement, 11, row, "kafka_topic")
              setInt(statement, 12, row, "kafka_partition")
              setLong(statement, 13, row, "kafka_offset")
              setTimestamp(statement, 14, row, "kafka_timestamp")
              setLong(statement, 15, row, "sequence")
              setTimestamp(statement, 16, row, "emitted_at")

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }

  // ============================================================
  // CREDIT
  // ============================================================

  def writeCredit(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

          connection.setAutoCommit(false)

          val sql =
            """
              INSERT INTO credit_events (
                event_id,
                customer_id,
                event_time,
                event_date,
                bureau,
                event_type,
                credit_score,
                score_change,
                enquiry_count,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at
              )
              VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
              )
              ON CONFLICT (event_id)
              DO NOTHING
            """

          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")

              setTimestamp(statement, 3, row, "event_time")
              setDate(statement, 4, row, "event_date")

              setString(statement, 5, row, "bureau")
              setString(statement, 6, row, "event_type")
              setInt(statement, 7, row, "credit_score")
              setInt(statement, 8, row, "score_change")
              setInt(statement, 9, row, "enquiry_count")

              setString(statement, 10, row, "customer_phone")

              setString(statement, 11, row, "kafka_topic")
              setInt(statement, 12, row, "kafka_partition")
              setLong(statement, 13, row, "kafka_offset")
              setTimestamp(statement, 14, row, "kafka_timestamp")
              setLong(statement, 15, row, "sequence")
              setTimestamp(statement, 16, row, "emitted_at")

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }

  // ============================================================
  // MARKETING
  // ============================================================

  def writeMarketing(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

          connection.setAutoCommit(false)

          val sql =
            """
              INSERT INTO marketing_events (
                event_id,
                customer_id,
                campaign_id,
                event_time,
                event_date,
                event_type,
                channel,
                product,
                message_id,
                device_id,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at
              )
              VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
              )
              ON CONFLICT (event_id)
              DO NOTHING
            """

          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")
              setString(statement, 3, row, "campaign_id")

              setTimestamp(statement, 4, row, "event_time")
              setDate(statement, 5, row, "event_date")

              setString(statement, 6, row, "event_type")
              setString(statement, 7, row, "channel")
              setString(statement, 8, row, "product")
              setString(statement, 9, row, "message_id")
              setString(statement, 10, row, "device_id")
              setString(statement, 11, row, "customer_phone")

              setString(statement, 12, row, "kafka_topic")
              setInt(statement, 13, row, "kafka_partition")
              setLong(statement, 14, row, "kafka_offset")
              setTimestamp(statement, 15, row, "kafka_timestamp")
              setLong(statement, 16, row, "sequence")
              setTimestamp(statement, 17, row, "emitted_at")

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }

  // ============================================================
  // CUSTOMER CDC
  // ============================================================

  def writeCustomerCdc(df: DataFrame): Unit = {

    df.foreachPartition {
      partition: Iterator[Row] =>

        withConnection { connection =>

          connection.setAutoCommit(false)

          val sql =
            """
              INSERT INTO customer_cdc (
                event_id,
                customer_id,
                event_time,
                event_date,
                operation,
                changed_field,
                old_value,
                new_value,
                source_system,
                customer_phone,
                kafka_topic,
                kafka_partition,
                kafka_offset,
                kafka_timestamp,
                sequence,
                emitted_at
              )
              VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?
              )
              ON CONFLICT (event_id)
              DO NOTHING
            """

          val statement =
            connection.prepareStatement(sql)

          try {

            var counter = 0

            partition.foreach { row =>

              setString(statement, 1, row, "event_id")
              setString(statement, 2, row, "customer_id")

              setTimestamp(statement, 3, row, "event_time")
              setDate(statement, 4, row, "event_date")

              setString(statement, 5, row, "operation")
              setString(statement, 6, row, "changed_field")
              setString(statement, 7, row, "old_value")
              setString(statement, 8, row, "new_value")
              setString(statement, 9, row, "source_system")
              setString(statement, 10, row, "customer_phone")

              setString(statement, 11, row, "kafka_topic")
              setInt(statement, 12, row, "kafka_partition")
              setLong(statement, 13, row, "kafka_offset")
              setTimestamp(statement, 14, row, "kafka_timestamp")
              setLong(statement, 15, row, "sequence")
              setTimestamp(statement, 16, row, "emitted_at")

              statement.addBatch()

              counter += 1

              if (counter % batchSize == 0) {
                statement.executeBatch()
                connection.commit()
                statement.clearBatch()
              }
            }

            if (counter % batchSize != 0) {
              statement.executeBatch()
              connection.commit()
              statement.clearBatch()
            }

          } catch {
            case exception: Exception =>
              connection.rollback()
              throw exception
          } finally {
            statement.close()
          }
        }
    }
  }
}