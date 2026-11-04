//go:build arm || 386 || mips || mipsle

package cp

// isASCII reports whether s contains only bytes in the ASCII range (0x00-0x7F).
func isASCII(s []byte) bool {
	for _, b := range s {
		if b >= 0x80 {
			return false
		}
	}
	return true
}
