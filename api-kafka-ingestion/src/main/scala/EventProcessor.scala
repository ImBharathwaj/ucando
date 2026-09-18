import model.EventEnvelope

class EventProcessor {
    def process(event: EventEnvelope): Unit ={
        println(
            s"EVENT_PROCESSED | " +
            s"dataset=${event.dataset} | " +
            s"topic=${event.topic} | " +
            s"customer_id=${event.payload.fields.get("customer_id")} | "
        )
    }
}