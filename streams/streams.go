package streams

// EventType represents DynamoDB streams event type in string values.
type EventType string

// Set of EventType values.
const (
	INSERT EventType = "INSERT"
	MODIFY EventType = "MODIFY"
)
