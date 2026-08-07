package fitz

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldNotDocumentNonexistentConnectOKGivenConnectMethodComment(t *testing.T) {
	// Arrange
	file, err := parser.ParseFile(token.NewFileSet(), "client.go", nil, parser.ParseComments)
	require.NoError(t, err)
	var comment string
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if ok && function.Name.Name == "Connect" && function.Doc != nil {
			comment = function.Doc.Text()
			break
		}
	}

	// Act
	documentsConnectOK := strings.Contains(comment, "CONNECT_OK")

	// Assert
	assert.NotEmpty(t, comment)
	assert.False(t, documentsConnectOK)
}
