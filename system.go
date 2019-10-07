package vbus

import (
	"github.com/godbus/dbus"
	"os"
)

func getHostname() (string, error) {
	hostname, err := tryGetHubHostname()
	if err == nil {
		return hostname, nil
	}

	return os.Hostname()
}

// Try to get the veea hub hostname using dbus service
func tryGetHubHostname() (string, error) {
	dbusconn, err := dbus.SystemBus()
	if err != nil {
		return "", err
	}

	hostname := ""
	obj := dbusconn.Object("io.veea.VeeaHub.Info","/io/veea/VeeaHub/Info")
	call := obj.Call("io.veea.VeeaHub.Info.Hostname", 0)
	if call.Err != nil {
		return "", call.Err
	}

	err = call.Store(&hostname)
	if err != nil {
		return "", err
	}

	return hostname, nil
}
