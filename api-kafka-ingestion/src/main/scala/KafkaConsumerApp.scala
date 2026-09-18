object KafkaConsumerApp {
    def main(args: Array[String]): Unit = {

        val processor = new EventProcessor()

        val dlqProducer = new KafkaDeadLetterProducer(
            bootstrapServers = "localhost:9092",
            deadLetterTopic = "bank.transactions.dlq"
        )

        val consumer = new KafkaConsumerService(
             bootstrapServers = "localhost:9092",
            groupId = "ucando-consumer",
            topic = "bank.transactions",
            processor = processor,
            dlqProducer = dlqProducer
        )

        sys.addShutdownHook{
            println("Shutting down consumer...")

            dlqProducer.close()
            consumer.close()
        }

        consumer.consume()
    }
}