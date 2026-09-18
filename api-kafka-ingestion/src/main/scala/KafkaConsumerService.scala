import org.apache.kafka.clients.consumer.{
    ConsumerConfig,
    KafkaConsumer,
    ConsumerRecords
}
import org.apache.kafka.common.serialization.StringDeserializer

import java.time.Duration
import java.util.Properties
import scala.collection.JavaConverters._

class KafkaConsumerService(
    bootstrapServers: String,
    groupId: String,
    topic: String,
    processor: EventProcessor,
    dlqProducer: KafkaDeadLetterProducer
){
    private val properties = new Properties()

    properties.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
        bootstrapServers
    )

    properties.setProperty(
        ConsumerConfig.GROUP_ID_CONFIG,
        groupId
    )

    properties.setProperty(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
        classOf[StringDeserializer].getName
    )

    properties.setProperty(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
        classOf[StringDeserializer].getName
    )

    properties.setProperty(
        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        "earliest"
    )

    properties.setProperty(
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        "false"
    )

    private val consumer = new KafkaConsumer[String, String](properties)

    consumer.subscribe(
        java.util.Collections.singletonList(topic)
    )

    def consume(): Unit = {
        while(true){
            val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(1000))

            for(record <- records.asScala){

                EventParser.parse(record.value()) match{
                    case Parsed(event) => 
                     processor.process(event)

                    // Business processing happens here

                    case ParseFailed(raw, reason) =>
                        println(
                            s"PARSE_FAILED | " +
                            s"partition=${record.partition()} | " +
                            s"offset=${record.offset()} | " +
                            s"reason=$reason"
                        )

                        dlqProducer.send(
                            key = record.key(),
                            value = raw
                        )
                }
            }
        }

        consumer.commitSync()

    }

    def close(): Unit ={
        consumer.close()
    }
}