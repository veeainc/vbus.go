package examples

import (
	"bitbucket.org/vbus/vbus.go"
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"os/signal"
	"strings"
	"sync"
	"testing"
	"time"
)

// Demonstrate how to add a method on Vbus
func TestMethod(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	err := client.Connect()
	assert.NoError(t, err)
	defer client.Close()

	// on server side
	_, err = client.AddMethod("scan", func(time int, parts []string) {
		t.Logf("called with %v", time)
	})

	// on client side
	scan, err := client.GetRemoteMethod("system", "testgo", client.GetHostname(), "scan")
	assert.NoError(t, err)
	_, _ = scan.CallWithTimeout(60, 42)

	WaitForCtrlC()
}

// Demonstrate how to create a node and subscribe on it
func TestNodes(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	err := client.Connect()
	assert.NoError(t, err)
	defer client.Close()

	onTimeoutSet := func(value interface{}, segments []string) {
		t.Logf("timeout setted with value %v", value)
	}

	scanMethod := func(time int, parts []string) string {
		t.Logf("called with %v", time)
		return "scanning"
	}

	// Create a node in one block
	// Automatically publish on vbus and attach callback.
	node, err := client.AddNode("00:45:25:65:25:ff", vBus.RawNode{
		"sub1": vBus.RawNode{
			"sub2": vBus.RawNode{
				"data":    "baz",
				"timeout": vBus.A("timeout", 500, vBus.OnSet(onTimeoutSet)),
				"scan":    vBus.M(scanMethod),
			},
		},
		"foo":  42,
		"name": vBus.A("name", "Eliott"),
	})
	assert.NoError(t, err)

	// update a value in the node above (send update on vbus)
	data, err := node.GetAttribute("sub1", "sub2", "data")
	assert.NoError(t, err)
	err = data.SetValue("hello world")

	// In another app
	attr, err := client.GetRemoteAttr("system", "testgo", client.GetHostname(), "00:45:25:65:25:ff", "sub1", "sub2", "timeout")
	assert.NoError(t, err)
	err = attr.SetValue(666)
	assert.NoError(t, err)

	scan, err := client.GetRemoteMethod("system", "testgo", client.GetHostname(), "00:45:25:65:25:ff", "sub1", "sub2", "scan")
	assert.NoError(t, err)
	resp, err := scan.Call(42)
	assert.NoError(t, err)
	assert.Equal(t, "scanning", resp)
}

func TestZigbeeScan(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	_ = client.Connect()

	deviceNodes, err := client.GetRemoteNode("system", "zigbee", client.GetHostname(), "devices")
	if err != nil {
		t.Error(err)
	}

	err = deviceNodes.SubscribeAdd(func(proxy *vBus.UnknownProxy, segments ...string) {
		fmt.Printf("IsAttribute: %v\n", proxy.IsAttribute())
		fmt.Printf("IsMethod: %v\n", proxy.IsMethod())
		fmt.Printf("IsNode: %v\n", proxy.IsNode())
		fmt.Println("device joined")
	})
	if err != nil {
		t.Error(err)
	}

	err = deviceNodes.SubscribeDel(func(proxy *vBus.UnknownProxy, segments ...string) {
		fmt.Println("device left")
	})
	if err != nil {
		t.Error(err)
	}

	scan, err := client.GetRemoteMethod("system", "zigbee", client.GetHostname(), "controller", "scan")
	if err != nil {
		t.Error(err)
	}

	res, err := scan.CallWithTimeout(60, 60)
	if err != nil {
		t.Error(err)
	}
	fmt.Printf("%v", res)

	WaitForCtrlC()
}

// An example on how to recursively traverse all nodes
func traverseNode(node *vBus.NodeProxy, level int) {
	for name, elem := range node.Elements() {
		if elem.IsNode() {
			n := elem.AsNode()
			fmt.Printf("%s%s:\n", strings.Repeat(" ", level * 2), name)
			traverseNode(n, level + 1)
		} else if elem.IsAttribute() {
			attr := elem.AsAttribute()
			fmt.Printf("%s%s = %v:\n", strings.Repeat(" ", level * 2), name, attr.Value())
		} else if elem.IsMethod() {
			fmt.Printf("%s%s:\n", strings.Repeat(" ", level * 2), name)
		}
	}
}

func TestZigbeeDiscover(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	err := client.Connect()
	assert.NoError(t, err)

	elem, err := client.Discover("system.zigbee", 1*time.Second)
	if err != nil {
		t.Error(err)
	}

	if elem.IsNode() {
		traverseNode(elem.AsNode(), 0)
	}

}

func TestExpose(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	err := client.Connect()
	assert.NoError(t, err)

	err = client.Expose("music", "https", 8000, "api/v2")
	assert.NoError(t, err)
	err = client.Expose("notifs", "https", 8000, "api/v2")
	assert.NoError(t, err)

	uris, err := client.GetRemoteNode("system", "testgo", client.GetHostname(), "uris")
	assert.NoError(t, err)
	fmt.Println(uris.String())

	WaitForCtrlC()
}

func WaitForCtrlC() {
	var end_waiter sync.WaitGroup
	end_waiter.Add(1)
	var signal_channel chan os.Signal
	signal_channel = make(chan os.Signal, 1)
	signal.Notify(signal_channel, os.Interrupt)
	go func() {
		<-signal_channel
		end_waiter.Done()
	}()
	end_waiter.Wait()
}
