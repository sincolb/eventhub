package eventhub

type (
	eventHubTableOptions struct {
		capacity   *int
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

func WithEventHubCapacity(size int) eventHubTableOption {
	return func(opts *eventHubTableOptions) {
		capacity := size
		opts.capacity = &capacity
	}
}
