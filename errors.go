package eventhub

import "errors"

var (
	ErrEventHubTableClosed   = errors.New("eventHubTable: closed")
	ErrEventHubNotSubscribed = errors.New("eventHubTable: not subscribed")

	ErrEventHubClosed     = errors.New("eventHub: closed")
	ErrEventHubTimeout    = errors.New("eventHub: timeout")
	ErrEventHubChanClosed = errors.New("eventHubChan: closed")
)
