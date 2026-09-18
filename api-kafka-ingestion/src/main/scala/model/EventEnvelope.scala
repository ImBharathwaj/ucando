package model

import spray.json._

final case class EventEnvelope(
    dataset: String,
    topic: String,
    emitted_at: String,
    sequence: Long,
    payload: JsObject
)

object EventEnvelope extends DefaultJsonProtocol {

    def toJsonString(event: EventEnvelope): String = 
        event.toJson.compactPrint
    
    implicit object EventEnvelopeFormat 
        extends RootJsonFormat[EventEnvelope] {

        override def write(event: EventEnvelope): JsValue = 
            JsObject(
                "dataset" -> JsString(event.dataset),
                "topic" -> JsString(event.topic),
                "emitted_at" -> JsString(event.emitted_at),
                "sequence" -> JsNumber(event.sequence),
                "payload" -> event.payload
            )

        override def read(json: JsValue): EventEnvelope = {
            val obj = json.asJsObject

            EventEnvelope(
                dataset = obj.fields("dataset").convertTo[String],
                topic = obj.fields("topic").convertTo[String],
                emitted_at = obj.fields("emitted_at").convertTo[String],
                sequence = obj.fields("sequence").convertTo[Long],
                payload = obj.fields("payload").asJsObject
            )
        }
    }
}