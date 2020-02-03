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
	p := setupTest(t, "./scenarios/ask_permission.json")
	defer p.Stop()

	client := assertNewClient(t)

	resp, err := client.AddAttribute("name", "HEIMAN")
	assert.NoError(t, err)
	assert.Equal(t, resp, true)

	assertPlayerSuccess(t, p)
}
