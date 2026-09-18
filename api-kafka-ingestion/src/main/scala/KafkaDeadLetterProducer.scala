import org.apache.kafka.clients.producer.{
    KafkaProducer,
    ProducerRecord,
    RecordMetadata
}

import java.util.Properties

class KafkaDeadLetterProducer(
    bootstrapServers: String,
    deadLetterTopic: String
){
    private val properties = new Properties()

    properties.setProperty(
        "bootstrap.servers",
        bootstrapServers
    )

    properties.setProperty(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
    )

    properties.setProperty(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
    )

    properties.setProperty(
        "acks",
        "all"
    )

    private val producer = new KafkaProducer[String, String](properties)

    def send(key: String, value: String): Unit = {
        val record = new ProducerRecord[String, String](
            deadLetterTopic,
            key,
            value
        )

        producer.send(
            record,
            (metadata: RecordMetadata, exception: Exception) => {
                if(exception!=null){
                     println(
                        s"DLQ_FAILURE | " +
                        s"topic=$deadLetterTopic | " +
                        s"reason=${exception.getMessage}"
                    )
                } else {
                    println(
                        s"DLQ_ACK | " +
                        s"topic=${metadata.topic()} | " +
                        s"partition=${metadata.partition()} | " +
                        s"offset=${metadata.offset()}"
                    )
                }
            }
        )
    }

    def close(): Unit = {
        producer.close()
    }
}