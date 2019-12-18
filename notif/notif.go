package notif

// Notifier is the interface that wraps the Notify method.
//
// Notify notifies the message to the output channel.
// The implementation channel can be slack/email/etc.
type Notifier interface {
	Notify(message string) error
}
