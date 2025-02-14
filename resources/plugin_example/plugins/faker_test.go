package plugins

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

const fakerConfYaml = `
username: true
age: true
batch_size: 10
`

var expectedMessages = []string{
	`{"age":82,"username":"sneak"}`,
	`{"age":26,"username":"einberger"}`,
	`{"age":13,"username":"mazy"}`,
	`{"age":81,"username":"rowdyism"}`,
	`{"age":21,"username":"destefano"}`,
	`{"age":83,"username":"clung"}`,
	`{"age":0,"username":"polypeptide"}`,
	`{"age":60,"username":"chungchungking"}`,
	`{"age":62,"username":"baerl"}`,
	`{"age":8,"username":"antimissile"}`,
}

func TestFakerInput(t *testing.T) {
	fakerConfigSpec := fakerInputSpec()

	pConf, err := fakerConfigSpec.ParseYAML(fakerConfYaml, nil)
	require.NoError(t, err)

	fakerInput, err := newFakerInput(pConf, []int{622}) // provide a seed for faker
	require.NoError(t, err)

	batch, _, err := fakerInput.ReadBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, 10, len(batch))

	for i, msg := range batch {
		b, _ := msg.AsBytes()

		require.Equal(t, expectedMessages[i], string(b))
	}
}
