package foobarbaz

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultLineParser(t *testing.T) {
	p := DefaultLineParser()
	record, err := p("myapp|a,b,c|p")
	require.Nil(t, err)
	require.NotNil(t, record)
	assert.Equal(t, "myapp", record.AppName)
	assert.Len(t, record.Destinations, 3)
	assert.Contains(t, record.Destinations, "a")
	assert.Contains(t, record.Destinations, "b")
	assert.Contains(t, record.Destinations, "c")
	assert.True(t, record.Publisher, "expected record to be from a publisher")
	assert.False(t, record.Consumer, "did not expect record to be from a consumer")
}
