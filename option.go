package mongotrace

import (
	"go.opencensus.io/trace"
)

type config struct {
	serviceName string
	sampler     trace.Sampler
}

// Option represents an option that can be passed to Dial.
type Option func(*config)

func defaults(cfg *config) {
	cfg.serviceName = "mongo"
}

// WithServiceName sets the given service name for the dialled connection.
// When the service name is not explicitly set it will be inferred based on the
// request to AWS.
func WithServiceName(name string) Option {
	return func(cfg *config) {
		cfg.serviceName = name
	}
}

// WithSampler set a sampler for all started spans.
func WithSampler(sampler trace.Sampler) Option {
	return func(cfg *config) {
		cfg.sampler = sampler
	}
}
