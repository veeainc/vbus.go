package tests

import (
	"github.com/stretchr/testify/assert"
	"testing"
)


func TestAskPermission(t *testing.T) {
	p := setupTest(t, "./scenarios/ask_permission.json")
	defer p.Stop()

	client := assertNewClient(t)

	// test success
	resp, err := client.AskPermission("should.be.true")
	assert.NoError(t, err)
	assert.Equal(t, resp, true)

	// test failure
	resp, err = client.AskPermission("should.be.false")
	assert.NoError(t, err)
	assert.Equal(t, resp, false)

	assertPlayerSuccess(t, p)
}

func TestAddAttribute(t *testing.T) {
	p := setupTest(t, "./scenarios/add_attribute.json")
	defer p.Stop()
	client := assertNewClient(t)

	attr, err := client.AddAttribute("name", "HEIMAN")
	assert.NoError(t, err)
	assert.NotNil(t, attr)

	assertPlayerSuccess(t, p)
}

func TestSetAttribute(t *testing.T) {
	p := setupTest(t, "./scenarios/set_attribute.json")
	defer p.Stop()
	client := assertNewClient(t)

	attr, err := client.AddAttribute("name", "HEIMAN")
	assert.NoError(t, err)
	assert.NotNil(t, attr)

	err = attr.SetValue("hello world")
	assert.NoError(t, err)

	assertPlayerSuccess(t, p)
}

func TestGetRemoteAttribute(t *testing.T) {
	p := setupTest(t, "./scenarios/remote_attribute_get.json")
	defer p.Stop()
	client := assertNewClient(t)

	remoteAttr, err := client.GetRemoteAttr("test", "remote", client.GetHostname(), "name")
	assert.NoError(t, err)
	assert.NotNil(t, remoteAttr)

	val, err := remoteAttr.ReadValue()
	assert.NoError(t, err)
	assert.Equal(t, "HEIMAN", val)

	assertPlayerSuccess(t, p)
}










