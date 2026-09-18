import model.EventEnvelope
import org.apache.kafka.clients.producer.{
    KafkaProducer,
    ProducerRecord,
    RecordMetadata
}

import spray.json._

import java.util.Properties

class KafkaProducerService(bootstrapServers: String){
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

    private val producer =  new KafkaProducer[String, String](properties)

    def send(event: EventEnvelope): Unit = {

        val key =
            event.payload.fields
                .get("customer_id")
                .collect {
                    case JsString(value) => value
                }
                .getOrElse(event.sequence.toString)

        val value = event.toJson.compactPrint

        val record = new ProducerRecord[String, String](
                event.topic,
                key,
                value
            )

        producer.send(
            record,
            (metadata: RecordMetadata, exception: Exception) => {

                if (exception != null) {
                        println(
                            s"PRODUCER_FAILURE | " +
                            s"topic=${event.topic} | " +
                            s"key=$key | " +
                            s"reason=${exception.getMessage}"
                        )
                } else {
                    println(
                        s"ACK | " +
                        s"topic=${metadata.topic()} | " +
                        s"partition=${metadata.partition()} | " +
                        s"offset=${metadata.offset()}"
                    )
                }
            }
        )
    }

    def close(): Unit = 
        producer.close()
}