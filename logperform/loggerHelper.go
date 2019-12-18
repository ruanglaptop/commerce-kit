package logperform

import (
	"fmt"
	"strings"
	"time"

	"github.com/payfazz/commerce-kit/notif"
	"github.com/payfazz/commerce-kit/types"
)

// FlagLog is type for represent Flagging for Log
type FlagLog string

const (
	// DONE is represent if log flag is Done
	DONE = "Done"
	// START is represent if log flag is Started
	START = "Started"
)

// MessageQueue is type of message queue
type MessageQueue []string

// Push is a function to push message into message queue list
func (mq *MessageQueue) Push(msg string) {
	// *mq = append(*mq, msg)
}

// MessagesQueue is global variable of message queue
var messagesQueue = MessageQueue(make([]string, 0))

// Pop is a function to pop message from message queue list
func (mq *MessageQueue) Pop() string {
	var result string
	// mqData := *mq
	// result, *mq = mqData[0], mqData[1:]

	return result
}

// MessageLoggerSender is function for notified logger to slack
func MessageLoggerSender(n *notif.SlackNotifier) {
	// for {
	// 	if len(messagesQueue) > 0 {
	// 		msg := messagesQueue.Pop()
	// 		if n != nil {
	// 			if err := n.Notify(fmt.Sprintf("```%s```", msg)); err != nil {
	// 				log.Println("NOTIFY TO SLACK ERROR: ", err)
	// 				messagesQueue = append(messagesQueue, msg)
	// 			}
	// 		}
	// 		time.Sleep(10 * time.Millisecond)
	// 	} else {
	// 		time.Sleep(10 * time.Second)
	// 	}
	// }
}

// LoggerStruct is struct for make PerformanceLogger
type LoggerStruct struct {
	CurrentStringLog *string
	Content          string
	CalledTime       *time.Duration
}

// LoggerFinal is struct for make PerformanceLoggerEnd
type LoggerFinal struct {
	LoggerData *LoggerStruct
	Method     *string
}

// PerformanceLogger is function for helping creating logger in string
func PerformanceLogger(loggerData *LoggerStruct) time.Time {
	var diffTime float64
	var logFormat string

	now := time.Now().UTC()

	timeNow := strings.Split(now.String(), "+")[0]

	if loggerData.CalledTime != nil {
		diffTime = loggerData.CalledTime.Seconds() * 1000
		logFormat = fmt.Sprintf("INFO: %v %s %s took %v ms", timeNow, loggerData.Content, DONE, diffTime)
	} else {
		logFormat = fmt.Sprintf("INFO: %v %s %s", timeNow, loggerData.Content, START)
	}

	if loggerData.CurrentStringLog == nil {
		logFormat = fmt.Sprintf("INFO: %v %s %s", timeNow, loggerData.Content, START)
		loggerData.CurrentStringLog = &logFormat
	} else {
		*loggerData.CurrentStringLog += "\n"
		*loggerData.CurrentStringLog += logFormat
	}

	return now
}

// PerformanceLoggerEnd is function for build all logger and send it to slack
func PerformanceLoggerEnd(now time.Time, loggerData *LoggerFinal, err *types.Error) {
	since := time.Since(now)

	loggerData.LoggerData.CalledTime = &since
	PerformanceLogger(loggerData.LoggerData)
	var status string

	if err != nil {
		status = "Error"
	} else {
		status = "Success"
	}

	// Pembentukkan akhir
	templateLog := fmt.Sprintf(
		"=====================================================================================\nMethod : %s\nTime Consumed: %v\nStatus: %s\nDetail :\n%s\n======================================================================================",
		*loggerData.Method,
		loggerData.LoggerData.CalledTime,
		status,
		*loggerData.LoggerData.CurrentStringLog)

	// notify ke slack
	messagesQueue.Push(templateLog)
}
