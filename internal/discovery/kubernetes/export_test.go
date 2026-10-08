package kubernetes

import "time"

const MinHealthyWatch = minHealthyWatch

func SetClock(p *Provider, now func() time.Time) {
	p.now = now
}

func SetBackoff(p *Provider, backoff func(attempt int) time.Duration) {
	p.backoff = backoff
}
