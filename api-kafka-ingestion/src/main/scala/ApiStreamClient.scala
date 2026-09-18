import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model._
import org.apache.pekko.stream.scaladsl.Framing
import org.apache.pekko.util.ByteString

import scala.concurrent.ExecutionContext

import spray.json._
import model.EventEnvelope
// import EventParser

object ApiStreamClient {
    def main(args: Array[String]): Unit = {
        implicit val system: ActorSystem = ActorSystem("api-ingestion")
        implicit val ec: ExecutionContext = system.dispatcher

        val kafkaProducer = new KafkaProducerService("localhost:9092")

        val uri = "http://localhost:8883/events"

        println(s"Connecting to $uri")

        val stream = Http()
        .singleRequest(HttpRequest(uri = uri))
        .flatMap { response =>

            println(s"HTTP response: ${response.status}")

        if (response.status.isSuccess()) {

            response.entity.dataBytes
            .via(
                Framing.delimiter(
                    ByteString("\n"),
                    maximumFrameLength = 1024 * 1024,
                    allowTruncation = false
                )   
            )
            .map(_.utf8String)
            .map(EventParser.parse)
            .map{
                case Parsed(event) => EventValidator.validate(event)
                case failed: ParseFailed => failed
            }
            .runForeach { 
                case Valid(event) =>
                    kafkaProducer.send(event)

                case Invalid(event, reason) =>
                    println(
                        s"INVALID | ${event.dataset} | reason=$reason"
                    )

                case failed: ParseFailed =>
                    println(
                        s"PARSE_FAILED | reason=${failed.reason} | raw=${failed.raw}"
                    )
            }

        } else {
            response.discardEntityBytes()
            throw new RuntimeException(
            s"API returned ${response.status}"
            )
        }
        }
        // Keep the application alive until the stream terminates.
        stream.onComplete{result=>
            println(s"Stream terminated: $result")

            kafkaProducer.close()
            
            system.terminate()
        }

        // Block the main thread.
        sys.addShutdownHook{
            println("Shutting down...")

            kafkaProducer.close()

            system.terminate()
        }

        scala.io.StdIn.readLine()
        system.terminate()

    }
}