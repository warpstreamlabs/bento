package helper

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type foo struct {
	name string
}

type page struct {
	fooList   []foo
	nextToken *string
}

type mockClient struct {
	pageMap map[string]page
	start   bool
}

func stringPtr(s string) *string {
	return &s
}

func (mc *mockClient) listFoo(token *string) (fooPage []foo, nextToken *string, err error) {
	if mc.start && token == nil {
		token = stringPtr("page1")
		mc.start = false
	}

	page, ok := mc.pageMap[*token]
	if !ok {
		return nil, nil, fmt.Errorf("invalid token: %v", *token)
	}

	return page.fooList, page.nextToken, nil
}

func TestCollect(t *testing.T) {
	tests := map[string]struct {
		testPageMap   map[string]page
		expResult     []foo
		expError      bool
		expErrorValue error
	}{
		"TestCollectSinglePage": {
			testPageMap: map[string]page{
				"page1": {fooList: []foo{{name: "Alice"}, {name: "Bob"}}, nextToken: nil},
			},
			expResult: []foo{{name: "Alice"}, {name: "Bob"}},
		},
		"TestCollectMultiplePages": {
			testPageMap: map[string]page{
				"page1": {fooList: []foo{{name: "Alice"}, {name: "Bob"}}, nextToken: stringPtr("page2")},
				"page2": {fooList: []foo{{name: "Carol"}, {name: "Dan"}}, nextToken: stringPtr("page3")},
				"page3": {fooList: []foo{{name: "Ethan"}, {name: "Frank"}}, nextToken: nil},
			},
			expResult: []foo{{name: "Alice"}, {name: "Bob"}, {name: "Carol"}, {name: "Dan"}, {name: "Ethan"}, {name: "Frank"}},
		},
		"TestCollectEmptyPages": {
			testPageMap: map[string]page{
				"page1": {fooList: []foo{}, nextToken: nil},
			},
			expResult: []foo{},
		},
		"TestCollectError": {
			testPageMap: map[string]page{
				"page1": {fooList: []foo{{name: "Alice"}, {name: "Bob"}}, nextToken: stringPtr("INVALID_TOKEN")},
				"page2": {fooList: []foo{{name: "Carol"}}, nextToken: nil},
			},
			expResult:     nil,
			expError:      true,
			expErrorValue: errors.New("invalid token: INVALID_TOKEN"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			client := mockClient{pageMap: test.testPageMap, start: true}

			shardIter := TokenIterator(func(token *string) ([]foo, *string, error) {
				return client.listFoo(token)
			})

			allFoo, err := Collect(shardIter)
			if test.expError {
				assert.Equal(t, test.expErrorValue, err)
			} else {
				require.NoError(t, err)
				require.ElementsMatch(t, test.expResult, allFoo)
			}
		})
	}
}
