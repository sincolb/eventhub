package eventhub

type (
	eventHubTableOptions struct {
		autoCommit bool
	}

	eventHubTableOption func(opts *eventHubTableOptions)
)

func buildEventHubTableOptions(opts ...eventHubTableOption) *eventHubTableOptions {
	options := &eventHubTableOptions{
		autoCommit: true,
	}
	for _, opt := range opts {
		opt(options)
	}

	return options
}

func WithEventHubTableAutoCommit(auto bool) eventHubTableOption {
	return func(opts *eventHubTableOptions) {
		opts.autoCommit = auto
	}
}
