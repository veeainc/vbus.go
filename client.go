package vBus

// The Vbus client. Library entry point.
type Client struct {
	*NodeManager
	//
	nats *ExtendedNatsClient
}

// Creates a new client with options.
func NewClient(domain, appId string, options ...NatsOption) *Client {
	nats := NewExtendedNatsClient(domain, appId, options...)
	return &Client{
		nats:        nats,
		NodeManager: NewNodeManager(nats),
	}
}

func (c *Client) GetHostname() string { return c.nats.GetHostname() }

func (c *Client) GetId() string { return c.nats.GetId() }

func (c *Client) Connect() error {
	err := c.nats.Connect()
	if err != nil {
		return err
	}

	return c.Initialize()
}

func (c *Client) AskPermission(permission string) (bool, error) {
	return c.client.AskPermission(permission)
}
