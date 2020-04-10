package vBus

// The Vbus client. Library entry point.
type Client struct {
	*NodeManager
	//
	nats    *ExtendedNatsClient
	options natsClientOptions
}

// ExtendedNatsClient options.
type natsClientOptions struct {
	StaticPath string
}

// Option is a function on the options for a connection.
type natsClientOption func(*natsClientOptions)

// Customize static path.
func WithStaticPath(staticPath string) natsClientOption {
	return func(o *natsClientOptions) {
		o.StaticPath = staticPath
	}
}

func getClientOptions(opt ...natsClientOption) natsClientOptions {
	opts := natsClientOptions{
		StaticPath: "./static",
	}
	for _, o := range opt {
		o(&opts)
	}
	return opts
}

// Creates a new client with options.
func NewClient(domain, appId string, opt ...natsClientOption) *Client {
	nats := NewExtendedNatsClient(domain, appId)
	return &Client{
		nats:        nats,
		NodeManager: NewNodeManager(nats),
		options:     getClientOptions(opt...),
	}
}

func (c *Client) GetHostname() string { return c.nats.GetHostname() }

func (c *Client) GetId() string { return c.nats.GetId() }

func (c *Client) Connect(options ...natsConnectOption) error {
	err := c.nats.Connect(options...)
	if err != nil {
		return err
	}

	return c.Initialize(c.options)
}

func (c *Client) AskPermission(permission string) (bool, error) {
	return c.client.AskPermission(permission)
}

// Retrieve client configuration (read only).
func (c *Client) GetConfig() (*configuration, error) {
	return c.client.readOrGetDefaultConfig()
}
