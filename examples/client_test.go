package examples

import (
	"bitbucket.org/vbus/vbus.go"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"testing"
	"time"
)


// Demonstrate how to add a method on Vbus
func TestMethod(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	err := client.Connect()
	if err != nil { t.Error(err) }

	// Register the method on Vbus
	_, err = client.AddMethod("scan", func(time int, parts []string) {
		fmt.Printf("called with %v", time)
	})


	scan, err := client.GetMethod("system", "testgo", client.GetHostname(), "scan")
	if err != nil {
		t.Error(err)
	}
	_, _ = scan.CallWithTimeout(60, 42)

	WaitForCtrlC()
}

func TestClient(t *testing.T) {
	// Call it with a proxy (simulate another app)
	clientApp := vBus.NewClient("system", "testclient")
	err := clientApp.Connect()
	if err != nil {
		t.Error(err)
	}

	scan, err := clientApp.GetMethod("system", "testgo", clientApp.GetHostname(), "scan")
	if err != nil {
		t.Error(err)
	}
	_, _ = scan.Call(42)
}

// Demonstrate how to create a node and subscribe on it
func TestNodes(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	_ = client.Connect()

	_, err := client.AddNode("00:45:25:65:25:ff", vBus.RawNode{
		"foo":  42,
		"name": vBus.A("name", "Eliott"),
	})

	t.Error(err)
}

func TestZigbeeScan(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	_ = client.Connect()

	deviceNodes, err := client.GetNode("system", "zigbee", client.GetHostname(), "devices")
	if err != nil { t.Error(err) }

	err = deviceNodes.SubscribeAdd(func(proxy *vBus.UnknownProxy, segments ...string) {
		fmt.Printf("IsAttribute: %v\n", proxy.IsAttribute())
		fmt.Printf("IsMethod: %v\n", proxy.IsMethod())
		fmt.Printf("IsNode: %v\n", proxy.IsNode())
		fmt.Println("device joined")
	})
	if err != nil { t.Error(err) }

	err = deviceNodes.SubscribeDel(func(proxy *vBus.UnknownProxy, segments ...string) {
		fmt.Println("device left")
	})
	if err != nil { t.Error(err) }

	scan, err := client.GetMethod("system", "zigbee", client.GetHostname(), "controller", "scan")
	if err != nil { t.Error(err) }

	res, err := scan.CallWithTimeout(60, 60)
	if err != nil { t.Error(err) }
	fmt.Printf("%v", res)

	WaitForCtrlC()
}

func TestZigbeeDiscover(t *testing.T) {
	client := vBus.NewClient("system", "testgo")
	_ = client.Connect()

	proxy, err := client.Discover("system.zigbee", 1 * time.Second)
	if err != nil { t.Error(err) }
	fmt.Printf("%s", proxy)
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