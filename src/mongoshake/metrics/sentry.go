package metrics

import "github.com/getsentry/raven-go"

func init() {
	raven.SetDSN("https://<key>:<secret>@sentry.io/<project>")
}