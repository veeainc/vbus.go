// This module contains connected nodes.
// A connected node is composed of a node definition and send commands over the Vbus when the
// user performs action on it. For example: add a child node, delete a node, call a method, etc...
package vBus

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
	"strings"
	"time"
)

const (
	NotifAdded   = "add"
	NotifRemoved = "del"
	NotifGet     = "get"
	NotifSetted  = "set"
)

// A Vbus connected node.
// It contains a node definition and send update over Vbus.
type Node struct {
	*NodeProxy
	client     *ExtendedNatsClient
	uuid       string
	definition *Definition
	parent     *Node
}

// Creates a Node.
func NewNode(nats *ExtendedNatsClient, uuid string, definition Definition) *Node {
	node := &Node{
		client:     nats,
		uuid:       uuid,
		definition: &definition,
		parent:     nil,
	}
	node.NodeProxy = NewNodeProxy(nats, node.GetPath(), definition.ToRepr())
	return node
}

// Creates a Node with parent.
func NewNodeWithParent(nats *ExtendedNatsClient, uuid string, definition Definition, parent *Node) *Node {
	node := &Node{
		client:     nats,
		uuid:       uuid,
		definition: &definition,
		parent:     parent,
	}
	node.NodeProxy = NewNodeProxy(nats, node.GetPath(), definition.ToRepr())
	return node
}

func (n *Node) GetUuid() string { return n.uuid }

// Returns the full path recursively.
func (n *Node) GetPath() string {
	if n.parent != nil {
		return joinPath(n.parent.GetPath(), n.uuid)
	} else {
		return n.uuid
	}
}

// Add a child node and notify Vbus
// Returns: a new node
func (n *Node) AddNode(uuid string, rawNode RawNode, option ...DefOption) (*Node, error) {
	definition := *n.definition
	nodeDef := NewNodeDef(rawNode, option...)
	definition.AddChild(uuid, nodeDef)

	// send the node definition on Vbus
	notif := JsonObj{uuid: nodeDef.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), NotifAdded), notif)
	if err != nil {
		return nil, errors.Wrap(err, "cannot publish new node")
	}

	return NewNodeWithParent(n.client, uuid, nodeDef, n), nil
}

// Add a child attribute and notify Vbus
// Returns: self (cannot add sub element to an attribute)
func (n *Node) AddAttribute(uuid string, value interface{}, options ...DefOption) (*Node, error) {
	definition := *n.definition
	nodeDef := NewAttributeDef(uuid, value, options...)
	definition.AddChild(uuid, nodeDef)

	// send the node definition on Vbus
	notif := JsonObj{uuid: nodeDef.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), NotifAdded), notif)
	if err != nil {
		return nil, errors.Wrap(err, "cannot publish new attribute")
	}

	return n, nil
}

// Add a child method node and notify Vbus
// Returns: self (cannot add sub element to method)
func (n *Node) AddMethod(uuid string, method MethodDefCallback) (*Node, error) {
	methodDef := NewMethodDef(method)
	definition := *n.definition
	definition.AddChild(uuid, methodDef)

	// send the node definition on Vbus
	notif := JsonObj{uuid: methodDef.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), NotifAdded), notif)
	if err != nil {
		return nil, errors.Wrap(err, "cannot publish new method")
	}

	return n, nil
}

// Remove a child node and notify Vbus
func (n *Node) RemoveNode(uuid string) error {
	definition := *n.definition
	deleted := definition.RemoveChild(uuid)

	if deleted == nil {
		log.Warnf("trying to delete unknown node %s", uuid)
		return nil
	}

	// send the node definition on Vbus
	notif := JsonObj{uuid: deleted.ToRepr()}
	err := n.client.Publish(joinPath(n.GetPath(), NotifRemoved), notif)
	if err != nil {
		return errors.Wrap(err, "cannot publish deleted node")
	}
	return nil
}

func (n *Node) String() string {
	return ToPrettyJson((*n.definition).ToRepr())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Node Manager
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// The NodeManager handle high level action like discovering nodes.
type NodeManager struct {
	*Node
	subs []*nats.Subscription
}

// Creates a new NodeManager. Don't forget to defer Close()
func NewNodeManager(nats *ExtendedNatsClient) *NodeManager {
	return &NodeManager{
		Node: NewNode(nats, "", NewNodeDef(RawNode{})),
	}
}

func (nm *NodeManager) Discover(natsPath string, timeout time.Duration) (*NodeProxy, error) {
	var resp JsonObj

	inbox := nm.client.client.NewRespInbox()

	sub, err := nm.client.client.Subscribe(inbox, func(msg *nats.Msg) {
		data, err := fromVbus(msg.Data)
		if err != nil {
			log.Warnf("received invalid node from %s", msg.Subject)
			return // skip
		}

		o, ok := data.(JsonObj)
		if !ok {
			log.Warn("received data is not a json object")
			return // skip
		}

		resp = mergeJsonObjs(resp, o)
	})
	if err != nil {
		return nil, errors.Wrap(err, "cannot subscribe to inbox")
	}

	err = nm.client.client.PublishRequest(natsPath, inbox, toVbus(nil))
	if err != nil {
		return nil, errors.Wrap(err, "cannot publish")
	}

	timer := time.NewTimer(timeout)
	<-timer.C

	_ = sub.Unsubscribe()
	_ = sub.Drain()

	return NewNodeProxy(nm.client, natsPath, resp), nil
}

func (nm *NodeManager) Initialize() error {
	// Subscribe to root path: "app-domain.app-name"
	sub, err := nm.client.Subscribe("", func(data interface{}, segments []string) interface{} {
		// get all nodes
		return JsonObj{nm.client.hostname: (*nm.definition).ToRepr()}
	}, WithoutHost())
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to root path")
	}
	nm.subs = append(nm.subs, sub) // save sub

	// Subscribe to all
	sub, err = nm.client.Subscribe(">", func(data interface{}, segments []string) interface{} {
		// Get a specific path
		parts := strings.Split(segments[0], ".") // split the first segment (">") to string list.
		if len(parts) < 2 {
			return nil // invalid path, missing event ("add", "del"...)
		}

		event, parts := parts[len(parts)-1], parts[:len(parts)-1] // pop event from parts
		return nm.handleEvent(data, event, parts...)
	})
	if err != nil {
		return errors.Wrap(err, "cannot subscribe to all")
	}
	nm.subs = append(nm.subs, sub) // save sub
	return nil
}

// Handle incoming get requests.
func (nm *NodeManager) handleEvent(data interface{}, event string, segments ...string) interface{} {
	nodeDef := (*nm.definition).SearchPath(segments)
	if nodeDef != nil { // if found
		var ret interface{}
		var err error

		switch event {
		case NotifGet:
			ret, err = nodeDef.HandleGet(data, segments)
		case NotifSetted:
			ret, err = nodeDef.HandleSet(data, segments)
		default:
			return nil
		}

		// Internal error
		if err != nil {
			log.Warnf("internal error while handling %s (%v)", joinPath(segments...), err.Error())
			return NewInternalError(err).ToRepr()
		}
		return ret
	} else { // Path not found
		log.Warnf("path not found %s", joinPath(segments...))
		return NewPathNotFoundErrorWithDetail(joinPath(segments...)).ToRepr()
	}
}

func (nm *NodeManager) Close() error {
	for _, sid := range nm.subs {
		err := sid.Unsubscribe()
		if err != nil {
			return errors.Wrap(err, "cannot unsubscribe from all")
		}
	}
	return nil
}
