package authz

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadTokens reads a token file mapping bearer tokens to identities, for token
// authentication as an alternative to mTLS client certificates. Each non-blank,
// non-comment (#) line is "token identity". Example:
//
//	# token                                identity
//	8f1c0a7e2b9d4c5f8a3e6b1d0c9f7a2e        orders-svc
//	1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d        dashboard
func LoadTokens(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("authz: open tokens file: %w", err)
	}
	defer f.Close()

	tokens := map[string]string{}
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 2 {
			return nil, fmt.Errorf("authz: %s line %d: want 'token identity', got %q", path, line, text)
		}
		tokens[fields[0]] = fields[1]
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("authz: read tokens file: %w", err)
	}
	return tokens, nil
}
