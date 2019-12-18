package mock

import (
	"fmt"

	"github.com/payfazz/commerce-kit/mailer"
)

// Mailer TODO: comment
type Mailer struct {
}

// SendEmail TODO: comment
func (m *Mailer) SendEmail(mailData mailer.MailData) {
	fmt.Printf(
		"Send email from \"%s\" to \"%s\" with subject \"%s\" and body \"%s\"\n",
		mailData.From, mailData.To, mailData.Subject, mailData.Body,
	)
}

// NewMailer TODO: comment
func NewMailer() (*Mailer, error) {
	return &Mailer{}, nil
}
