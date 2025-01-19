package eventhub

import (
	"time"
)

type eventPayload struct {
	payload  any
	lastTime time.Time
	life     time.Duration
}

func (p *eventPayload) Payload() (any, bool) {
	if p.payload != nil && (p.life == 0 ||
		p.lastTime.After(time.Now().Add(-p.life))) {
		return p.payload, true
	}
	return nil, false
}
