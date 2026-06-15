package authz

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// LoadFile reads an ACL file into an Authorizer. Each non-blank, non-comment (#)
// line is "identity prefix mode", where prefix is a key-path prefix (and "*" means
// all keys) and mode is "r" optionally followed by "w" (write) and/or "a" (cluster
// administration — AddNode/RemoveNode; never implied by w). Every principal also
// implicitly has read-write on its own /<identity>/ subtree (see HomeDir), so the
// file need only grant additional cross-user access. Example:
//
//	# identity   prefix            mode
//	bob          /alice/configs/   r
//	svc-orders   /shared/orders/   rw
//	operator     *                 rwa
func LoadFile(path string) (Authorizer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("authz: open ACL file: %w", err)
	}
	defer f.Close()

	rules := map[string][]Rule{}
	sc := bufio.NewScanner(f)
	for line := 1; sc.Scan(); line++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		fields := strings.Fields(text)
		if len(fields) != 3 {
			return nil, fmt.Errorf("authz: %s line %d: want 'identity prefix mode', got %q", path, line, text)
		}
		id, prefix, mode := fields[0], fields[1], fields[2]
		if prefix == "*" {
			prefix = ""
		}
		rule, err := parseMode(mode)
		if err != nil {
			return nil, fmt.Errorf("authz: %s line %d: %w", path, line, err)
		}
		rule.Prefix = prefix
		rules[id] = append(rules[id], rule)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("authz: read ACL file: %w", err)
	}
	// Every principal owns its own /<identity>/ subtree (read-write) implicitly; the
	// file rules grant only additional cross-user access on top of that.
	return HomeDir(Prefix(rules)), nil
}

// parseMode parses an ACL mode string: "r" plus optional "w" and/or "a".
func parseMode(mode string) (Rule, error) {
	if mode == "" || mode[0] != 'r' {
		return Rule{}, fmt.Errorf("mode %q must start with r (e.g. r, rw, rwa)", mode)
	}
	var r Rule
	for _, c := range mode[1:] {
		switch c {
		case 'w':
			r.Write = true
		case 'a':
			r.Admin = true
		default:
			return Rule{}, fmt.Errorf("mode %q contains unknown right %q (allowed: r, w, a)", mode, string(c))
		}
	}
	return r, nil
}
