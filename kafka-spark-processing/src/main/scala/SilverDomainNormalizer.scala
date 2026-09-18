import model.BankTransactionSchema
import model.CustomerBehaviorSchema
import model.LoanEventSchema
import model.InsuranceEventSchema
import model.CreditEventSchema
import model.MarketingEventSchema
import model.CustomerProfileSchema

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object SilverDomainNormalizer {

  // ============================================================
  // BANK TRANSACTIONS
  // ============================================================

  def transactions(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "bank_transaction")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          BankTransactionSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),

        col("event.payload.account_id").alias("account_id"),
        col("event.payload.transaction_id")
          .alias("transaction_id"),

        col("event.payload.transaction_type")
          .alias("transaction_type"),

        col("event.payload.amount")
          .alias("amount"),

        col("event.payload.currency")
          .alias("currency"),

        col("event.payload.merchant_id")
          .alias("merchant_id"),

        col("event.payload.merchant_name")
          .alias("merchant_name"),

        col("event.payload.merchant_category")
          .alias("merchant_category"),

        col("event.payload.channel")
          .alias("channel"),

        col("event.payload.location")
          .alias("location"),

        col("event.payload.balance_after")
          .alias("balance_after"),

        col("event.payload.customer_phone")
          .alias("customer_phone"),

        to_timestamp(
          col("event.payload.event_time")
        ).alias("event_time"),

        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("sequence"),
        col("emitted_at"),

        col("event_date")
      )
  }

  // ============================================================
  // CUSTOMER BEHAVIOR
  // ============================================================

  def behavior(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "customer_behavior")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          CustomerBehaviorSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),

        col("event.payload.session_id")
          .alias("session_id"),

        col("event.payload.device_id")
          .alias("device_id"),

        col("event.payload.channel")
          .alias("channel"),

        col("event.payload.event_type")
          .alias("event_type"),

        col("event.payload.page")
          .alias("page"),

        col("event.payload.product")
          .alias("product"),

        col("event.payload.campaign_id")
          .alias("campaign_id"),

        col("event.payload.source")
          .alias("source"),

        col("event.payload.device_type")
          .alias("device_type"),

        col("event.payload.ip_country")
          .alias("ip_country"),

        col("event.payload.customer_phone")
          .alias("customer_phone"),

        to_timestamp(
          col("event.payload.event_time")
        ).alias("event_time"),

        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("sequence"),
        col("emitted_at"),

        col("event_date")
      )
  }

  // ============================================================
  // LOANS
  // ============================================================

  def loans(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "loan")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          LoanEventSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),

        col("event.payload.loan_id")
          .alias("loan_id"),

        col("event.payload.application_id")
          .alias("application_id"),

        col("event.payload.event_type")
          .alias("event_type"),

        col("event.payload.loan_type")
          .alias("loan_type"),

        col("event.payload.amount")
          .alias("amount"),

        col("event.payload.status")
          .alias("status"),

        col("event.payload.channel")
          .alias("channel"),

        col("event.payload.customer_phone")
          .alias("customer_phone"),

        to_timestamp(
          col("event.payload.event_time")
        ).alias("event_time"),

        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("sequence"),
        col("emitted_at"),

        col("event_date")
      )
  }

  // ============================================================
  // INSURANCE
  // ============================================================

  def insurance(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "insurance")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          InsuranceEventSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),

        col("event.payload.policy_id")
          .alias("policy_id"),

        col("event.payload.event_type")
          .alias("event_type"),

        col("event.payload.policy_type")
          .alias("policy_type"),

        col("event.payload.premium_amount")
          .alias("premium_amount"),

        col("event.payload.channel")
          .alias("channel"),

        col("event.payload.customer_phone")
          .alias("customer_phone"),

        to_timestamp(
          col("event.payload.event_time")
        ).alias("event_time"),

        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("sequence"),
        col("emitted_at"),

        col("event_date")
      )
  }

  // ============================================================
  // CREDIT
  // ============================================================

  def credit(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "credit")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          CreditEventSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),

        col("event.payload.bureau")
          .alias("bureau"),

        col("event.payload.event_type")
          .alias("event_type"),

        col("event.payload.credit_score")
          .alias("credit_score"),

        col("event.payload.score_change")
          .alias("score_change"),

        col("event.payload.enquiry_count")
          .alias("enquiry_count"),

        col("event.payload.customer_phone")
          .alias("customer_phone"),

        to_timestamp(
          col("event.payload.event_time")
        ).alias("event_time"),

        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("sequence"),
        col("emitted_at"),

        col("event_date")
      )
  }

  // ============================================================
  // MARKETING
  // ============================================================

  def marketing(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "marketing")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          MarketingEventSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),
        col("event.payload.campaign_id").alias("campaign_id"),
        col("event.payload.event_type").alias("event_type"),
        col("event.payload.channel").alias("channel"),
        col("event.payload.product").alias("product"),
        col("event.payload.message_id").alias("message_id"),
        col("event.payload.device_id").alias("device_id"),
        col("event.payload.customer_phone").alias("customer_phone"),
        to_timestamp(col("event.payload.event_time")).alias("event_time"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        col("sequence"),
        col("emitted_at"),
        col("event_date")
      )
  }

  // ============================================================
  // CUSTOMER CDC
  // ============================================================

  def customerCdc(df: DataFrame): DataFrame = {

    df
      .filter(col("event_domain") === "customer_cdc")
      .withColumn(
        "event",
        from_json(
          col("raw_event"),
          CustomerProfileSchema.schema
        )
      )
      .select(
        col("event.payload.event_id").alias("event_id"),
        col("event.payload.customer_id").alias("customer_id"),

        col("event.payload.operation")
          .alias("operation"),

        col("event.payload.changed_fields.field")
          .alias("changed_field"),

        col("event.payload.changed_fields.old_value")
          .alias("old_value"),

        col("event.payload.changed_fields.new_value")
          .alias("new_value"),

        col("event.payload.source_system")
          .alias("source_system"),

        col("event.payload.customer_phone")
          .alias("customer_phone"),

        to_timestamp(
          col("event.payload.event_time")
        ).alias("event_time"),

        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),

        col("sequence"),
        col("emitted_at"),

        col("event_date")
      )
  }
}