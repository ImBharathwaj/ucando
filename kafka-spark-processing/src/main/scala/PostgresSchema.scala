import java.sql.Connection

object PostgresSchema{
    def createTables(connection: Connection): Unit = {
        val statement = connection.createStatement()

        // Customer
        statement.execute(
            """
            CREATE TABLE IF NOT EXISTS customers (
                customer_id TEXT PRIMARY KEY,
                customer_phone TEXT,
                last_event_time TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        // Transactions
        statement.execute(
            """
            CREATE TABLE IF NOT EXISTS transactions (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                account_id TEXT,
                transaction_id TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                transaction_type TEXT,
                amount DOUBLE PRECISION,
                currency TEXT,
                merchant_id TEXT,
                merchant_name TEXT,
                merchant_category TEXT,
                channel TEXT,
                location TEXT,
                balance_after DOUBLE PRECISION,
                customer_phone TEXT,
                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
            )
            """
        )

        // Behavior

        statement.execute(
            """
                CREATE TABLE IF NOT EXISTS behavior_events (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                session_id TEXT,
                device_id TEXT,
                channel TEXT,
                event_type TEXT,
                page TEXT,
                product TEXT,
                campaign_id TEXT,
                source TEXT,
                device_type TEXT,
                ip_country TEXT,
                customer_phone TEXT,
                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
                )
            """
        )

        // Loans

        statement.execute(
            """
                CREATE TABLE IF NOT EXISTS loans (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                loan_id TEXT,
                application_id TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                event_type TEXT,
                loan_type TEXT,
                amount DOUBLE PRECISION,
                status TEXT,
                channel TEXT,
                customer_phone TEXT,

                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
                )
            """
        )

        // Insurance

        statement.execute(
            """
                CREATE TABLE IF NOT EXISTS insurance_policies (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                policy_id TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                event_type TEXT,
                policy_type TEXT,
                premium_amount DOUBLE PRECISION,
                channel TEXT,
                customer_phone TEXT,
                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
                )
            """
        )

        // Credit

        statement.execute(
            """
                CREATE TABLE IF NOT EXISTS credit_events (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                bureau TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                event_type TEXT,
                credit_score INT,
                score_change INT,
                enquiry_count INT,
                customer_phone TEXT,
                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
                )
            """
        )

        // Marketing

        statement.execute(
            """
                CREATE TABLE IF NOT EXISTS marketing_events (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                campaign_id TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                event_type TEXT,
                channel TEXT,
                product TEXT,
                message_id TEXT,
                device_id TEXT,
                customer_phone TEXT,
                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
                )
            """
        )

        // Customer CDC

        statement.execute(
            """
                CREATE TABLE IF NOT EXISTS customer_cdc (
                event_id TEXT PRIMARY KEY,
                customer_id TEXT,
                event_time TIMESTAMP,
                event_date DATE,
                operation TEXT,
                changed_field TEXT,
                old_value TEXT,
                new_value TEXT,
                source_system TEXT,
                customer_phone TEXT,
                kafka_topic TEXT,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP,
                sequence BIGINT,
                emitted_at TIMESTAMP
                )
            """
        )

        // Indexes
        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_customer
            ON transactions(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_behavior_customer
            ON behavior_events(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_loans_customer
            ON loans(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_insurance_customer
            ON insurance_policies(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transactions_customer
            ON transactions(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_credit_customer
            ON credit_events(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_marketing_customer
            ON marketing_events(customer_id)
            """
        )

        statement.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_cdc_customer
            ON customer_cdc(customer_id)
            """
        )

        statement.close()
    }
}