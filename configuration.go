package vBus

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"

	"github.com/pkg/errors"
)

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Configuration
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// For now only vbus route
type vbusRoute struct {
	Url       string `json:"url"`
	NetworkIp string `json:"networkIp"`
	Hostname  string `json:"hostname"`
}

type clientConfiguration struct {
	Vbus vbusRoute `json:"vbus"`
}

// Try to read config file.
// If not found, it returns the default configuration.
func (c *ExtendedNatsClient) getClientConfig() (*clientConfiguration, error) {
	if _, err := os.Stat(c.rootFolder); os.IsNotExist(err) {
		err = os.Mkdir(c.rootFolder, os.ModeDir)
		if err != nil {
			return nil, err
		}
	}

	_natsLog.WithField("filepath", c.rootFolder).Debug("check if we already have a Vbus config file")
	configFile := path.Join(c.rootFolder, c.id) + ".conf"
	if fileExists(configFile) {
		_natsLog.WithField("id", c.id).Debug("load existing configuration file")
		jsonFile, err := os.Open(configFile)
		if err != nil {
			return nil, errors.Wrap(err, "cannot open config file")
		}

		bytes, err := ioutil.ReadAll(jsonFile)
		if err != nil {
			return nil, errors.Wrap(err, "cannot read config file")
		}

		var config clientConfiguration
		err = json.Unmarshal(bytes, &config)
		if err != nil {
			return nil, errors.Wrap(err, "cannot parse config file")
		}
		return &config, nil
	} else {
		_natsLog.WithField("id", c.id).Debug("create new configuration file")
		return c.createConfig()
	}
}

// Creates a default configuration object.
func (c *ExtendedNatsClient) createConfig() (*clientConfiguration, error) {
	_natsLog.WithField("id", c.id).Debug("create new client configuration file")

	return &clientConfiguration{}, nil
}

// Write configuration on disk
func (c *ExtendedNatsClient) saveClientsConfigFile(config *clientConfiguration) error {
	data := toVbus(config)
	filepath := path.Join(c.rootFolder, c.id+".conf")
	return ioutil.WriteFile(filepath, data, 0666)
}
