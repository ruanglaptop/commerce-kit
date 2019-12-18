package response

import (
	"fmt"
	"strings"

	"github.com/payfazz/commerce-kit/notif"
)

// Notification represents message
type Notification string

// Notify notify to slack
func Notify(n notif.Notifier, notifications []*Notification) {
	var notificationsStr []string
	for _, singleNotification := range notifications {
		notification := string(*singleNotification)
		notificationsStr = append(notificationsStr, notification)
	}
	notifyMessage := strings.Join(notificationsStr, "\n\n")
	if err := n.Notify(fmt.Sprintf("```%s```", notifyMessage)); err != nil {
		fmt.Println("NOTIFY TO SLACK ERROR: ", err)
	}
}
