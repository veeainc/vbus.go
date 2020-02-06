package tests

import (
	"bitbucket.org/vbus/nats-scenario-player/player"
	vBus "bitbucket.org/vbus/vbus.go"
	"github.com/stretchr/testify/assert"
	"os"
	"os/user"
	"testing"
)

func setupTest(t *testing.T, scenario string) *player.NatsPlayer {
	u, err := user.Current()
	assert.NoError(t, err)
	file := u.HomeDir + "/vbus/test.vbusgo.conf"
	if fileExists(file) {
		err = os.Remove(file)
		assert.NoError(t, err)
	}

	p := player.New("test.vbusgo")
	err = p.Load(scenario)
	assert.NoError(t, err)
	err = p.Play()
	assert.NoError(t, err)
	return p
}

func assertPlayerSuccess(t *testing.T, p *player.NatsPlayer) {
	p.WaitDone()

	for _, r := range p.GetResults() {
		t.Log(r.String())
	}

	p.WriteJUnitReport(t.Name() + "_junit.xml")
	assert.True(t, p.IsSuccess())
}

// Create and connect a new Vbus client.
func assertNewClient(t *testing.T) *vBus.Client {
	vbus := vBus.NewClient("test", "vbusgo")
	err := vbus.Connect()
	assert.NoError(t, err)
	return vbus
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
