package notif

import (
	"context"
	"errors"

	"github.com/payfazz/messenger/internal/provider/mail/ses"
	"github.com/payfazz/messenger/internal/provider/sender"
)

// EmailNotifier email notifier using payfazz messenger as library
type EmailNotifier struct {
	emailNotifier sender.Sender
}

// Notify Notify function are not implemented on email
func (en *EmailNotifier) Notify(message string) error {
	return errors.New("Notify function are not implemented on email")
}

// Send sending email
func (en *EmailNotifier) Send(ctx context.Context, data interface{}) error {
	err := en.emailNotifier.Send(ctx, data)
	if err != nil {
		return err
	}

	return nil
}

// NewEmailNotifier create new email notifier
func NewEmailNotifier(
	name string,
	region string,
	prefixes []string,
) *EmailNotifier {
	sesEmailServices := ses.NewSESSender(
		name,
		region,
		prefixes,
	)
	return &EmailNotifier{
		emailNotifier: sesEmailServices,
	}
}
