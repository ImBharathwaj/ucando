import model.EventEnvelope

sealed trait ValidationResult

final case class Valid(event: EventEnvelope) extends ValidationResult

final case class Invalid(
    event: EventEnvelope,
    reason: String
) extends ValidationResult


object EventValidator {
    def validate(event: EventEnvelope): ValidationResult = {
        if(event.dataset.trim.isEmpty)
            return Invalid(event, "dataset is empty")

        if(event.topic.trim.isEmpty)
            return Invalid(event, "topic is empty")
            
        if(event.sequence < 0)
            return Invalid(event, "sequence cannot be negative")

        if(event.payload.fields.isEmpty)
            return Invalid(event, "payload is empty")
            
        Valid(event)
    }
}