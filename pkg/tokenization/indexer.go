package tokenization

import (
	"github.com/daulet/tokenizers"
)

// Indexer interface defines the methods for managing tokenization data.
// It allows looking up the longest tokenization prefix for a given
// model-name and prompt.
type Indexer interface {
	// AddTokenization adds the full tokenization of a string to the
	// indexer for a given model.
	// The function assumes tokens and offsets are of the same length.
	// The function assumes that tokens will not be mutated after the call.
	AddTokenization(modelName string, prompt string, tokens []uint32, offsets []tokenizers.Offset) error
	// FindLongestContainedTokens finds the sequence of contained tokens for
	// the longest matching prefix.
	FindLongestContainedTokens(prompt, modelName string) []uint32
}
