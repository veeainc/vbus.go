// Proxies are object used to communicate with a remote VBus element.
// For example, reading a remote attribute, calling a remote method.
package vBus

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"time"
)

// Represents a generic proxy (i.e. a method, an attribute...)
type IProxy interface {
	// Get the full path.
	GetPath() string

	// Get the name (last part of the path).
	GetName() string

	// Get the string representation
	String() string
}

// Subscription callback type.
type ProxySubCallback = func(proxy *UnknownProxy, segments ...string)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Proxy Base Struct
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Define actions and data available for all proxy.
type ProxyBase struct { // implements IProxy
	client *ExtendedNatsClient
	path   string
	sids   []*nats.Subscription
	name   string
	rawDef JsonAny
}

func NewProxyBase(client *ExtendedNatsClient, path string, rawDef JsonAny) *ProxyBase {
	return &ProxyBase{
		client: client,
		path:   path,
		sids:   []*nats.Subscription{},
		name:   lastSplit(path, "."),
		rawDef: rawDef,
	}
}

// The default is json marshalling
func (p *ProxyBase) String() string {
	return ToPrettyJson(p.rawDef)
}

// Return the full path.
func (p *ProxyBase) GetPath() string { return p.path }

// Get element name (last part of path).
func (p *ProxyBase) GetName() string { return p.name }

// Unsubscribe from all
func (p *ProxyBase) Unsubscribe() error {
	for _, sid := range p.sids {
		err := sid.Unsubscribe()
		if err != nil {
			return errors.Wrap(err, "cannot unsubscribe from all")
		}
	}
	return nil
}

// Generic subscribe.
// It subscribe to an event (i.e. "add", "del", etc...) with a callback.
func (p *ProxyBase) subscribeToEvent(cb ProxySubCallback, event string, parts ...string) error {
	natsPath := joinPath(p.path, joinPath(parts...), event)

	sub, err := p.client.Subscribe(natsPath, func(rawNode interface{}, segments []string) interface{} {
		if js, ok := rawNode.(JsonObj); ok {
			node := NewUnknownProxy(p.client, p.path, js)
			cb(node, segments...)
		}
		return nil
	}, WithoutId(), WithoutHost())

	p.sids = append(p.sids, sub) // save subscription
	return err
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Unknown Proxy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// When we don't know in advance the object type, we use an UnknownProxy.
// For example, when we subscribe to a path, the library will return an UnknownProxy.
// Then you will have to assert it to the correct type using IsAttribute, IsMethod...
type UnknownProxy struct { // implements IProxy
	*ProxyBase
}

func NewUnknownProxy(client *ExtendedNatsClient, path string, rawNode JsonObj) *UnknownProxy {
	return &UnknownProxy{
		ProxyBase: NewProxyBase(client, path, rawNode),
	}
}

// Is it an attribute ?
func (up *UnknownProxy) IsAttribute() bool {
	return isAttribute(&up.rawDef)
}

// Transform to an AttributeProxy (use IsAttribute before).
func (up *UnknownProxy) AsAttribute() *AttributeProxy {
	return NewAttributeProxy(up.client, up.path, up.rawDef.(JsonObj))
}

// Is it a method ?
func (up *UnknownProxy) IsMethod() bool {
	return isMethod(&up.rawDef)
}

// Transform to an MethodProxy (use IsMethod before).
func (up *UnknownProxy) AsMethod() *MethodProxy {
	return NewMethodProxy(up.client, up.path, up.rawDef.(JsonObj))
}

// Is it a node ?
func (up *UnknownProxy) IsNode() bool {
	return isNode(&up.rawDef)
}

// Transform to an NodeProxy (use IsMethod before).
func (up *UnknownProxy) AsNode() *NodeProxy {
	return NewNodeProxy(up.client, up.path, up.rawDef.(JsonObj))
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Attribute Proxy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Represents remote node actions.
type AttributeProxy struct {
	*ProxyBase
	rawAttr JsonObj
}

func NewAttributeProxy(client *ExtendedNatsClient, path string, rawNode JsonObj) *AttributeProxy {
	return &AttributeProxy{
		ProxyBase: NewProxyBase(client, path, rawNode),
		rawAttr:   rawNode,
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Node Proxy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Represents remote node actions.
type NodeProxy struct {
	*ProxyBase
	rawNode JsonObj
}

func NewNodeProxy(client *ExtendedNatsClient, path string, rawNode JsonObj) *NodeProxy {
	return &NodeProxy{
		ProxyBase: NewProxyBase(client, path, rawNode),
		rawNode:   rawNode,
	}
}

func (np *NodeProxy) Tree() JsonObj {
	return np.rawNode
}

// Subscribe to the add event.
func (np *NodeProxy) SubscribeAdd(cb ProxySubCallback, parts ...string) error {
	return np.subscribeToEvent(cb, NotifAdded, parts...)
}

// Subscribe to the del event.
func (np *NodeProxy) SubscribeDel(cb ProxySubCallback, parts ...string) error {
	return np.subscribeToEvent(cb, NotifRemoved, parts...)
}

// Retrieve a node proxy
func (np *NodeProxy) GetNode(parts ...string) (*NodeProxy, error) {
	if isWildcardPath(parts...) {
		panic("Wildcard proxy not yet implemented")
	} else {
		rawElementDef := getPathInObj(np.rawNode, parts...)
		if rawElementDef != nil {
			return NewNodeProxy(np.client, joinPath(prepend(np.GetPath(), parts)...), rawElementDef), nil
		} else {
			// load from Vbus
			resp, err := np.client.Request(joinPath(append(parts, NotifGet)...), nil, WithoutHost(), WithoutId())
			if err != nil {
				return nil, errors.Wrap(err, "cannot retrieve remote node")
			}
			// check if its a json object
			if rawElementDef, ok := resp.(JsonObj); ok {
				return NewNodeProxy(np.client, joinPath(prepend(np.GetPath(), parts)...), rawElementDef), nil
			}
			return nil, errors.New("Retrieved value on Vbus is not a valid json node")
		}
	}
}

// Retrieve a method proxy
func (np *NodeProxy) GetMethod(parts ...string) (*MethodProxy, error) {
	if isWildcardPath(parts...) {
		panic("cannot use a wildcard path")
	} else {
		rawElementDef := getPathInObj(np.rawNode, parts...)
		if rawElementDef != nil {
			return NewMethodProxy(np.client, joinPath(prepend(np.GetPath(), parts)...), rawElementDef), nil
		} else {
			// load from Vbus
			resp, err := np.client.Request(joinPath(append(parts, NotifGet)...), nil, WithoutHost(), WithoutId(), Timeout(2*time.Second))
			if err != nil {
				return nil, errors.Wrap(err, "cannot retrieve remote node")
			}
			// check if its a json object
			if rawElementDef, ok := resp.(JsonObj); ok {
				return NewMethodProxy(np.client, joinPath(prepend(np.GetPath(), parts)...), rawElementDef), nil
			}
			return nil, errors.New("Retrieved value on Vbus is not a valid json node")
		}
	}
}
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Method Proxy
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Represents remote method actions
type MethodProxy struct { // implements IProxy
	*ProxyBase
	methodDef JsonObj
}

func NewMethodProxy(client *ExtendedNatsClient, path string, methodDef JsonObj) *MethodProxy {
	return &MethodProxy{
		ProxyBase: NewProxyBase(client, path, methodDef),
		methodDef: methodDef,
	}
}

// Call this method with some arguments.
func (mp *MethodProxy) Call(args ...interface{}) (interface{}, error) {
	return mp.client.Request(joinPath(mp.path, NotifSetted), args, WithoutHost(), WithoutId())
}

// Call this method with some arguments and a timeout.
func (mp *MethodProxy) CallWithTimeout(timeout time.Duration, args ...interface{}) (interface{}, error) {
	return mp.client.Request(joinPath(mp.path, NotifSetted), args, Timeout(timeout), WithoutHost(), WithoutId())
}
