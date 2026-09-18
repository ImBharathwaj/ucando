import model.EventEnvelope
import spray.json._

sealed trait ParseResult

final case class Parsed(event: EventEnvelope) extends ParseResult

final case class ParseFailed(
    raw: String,
    reason: String
) extends ParseResult

object EventParser {
    def parse(line: String): ParseResult = {
        try {
            Parsed(
                line
                    .parseJson
                    .convertTo[EventEnvelope]
            )
        } catch {
            case e: Exception => 
                ParseFailed(
                    raw = line,
                    reason = e.getMessage
                )
        }
    }
}