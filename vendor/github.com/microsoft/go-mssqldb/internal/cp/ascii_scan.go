//go:build !386 && !arm && !mips && !mipsle

package cp

import "encoding/binary"

// asciiMask64 has the high bit set in each byte. When AND-ed with an 8 byte
// chunk it reports whether any byte in that chunk has its high bit set, i.e.
// is >= 0x80.
const asciiMask64 uint64 = 0x8080808080808080

// isASCII reports whether s contains only bytes in the ASCII range (0x00-0x7F).
// It scans the input in 8 byte chunks, the same pattern used by ucs22str in the
// parent package.
func isASCII(s []byte) bool {
	// how many 8 byte chunks are in the input buffer
	nlen8 := len(s) &^ 7
	for i := 0; i < nlen8; i += 8 {
		// read the 8 byte chunk with an alignment-safe load
		ui64 := binary.NativeEndian.Uint64(s[i : i+8])

		// mask the entire 64 bit region and check for any byte with the high bit set
		if ui64&asciiMask64 > 0 {
			return false
		}
	}

	// deal with the at most 7 remaining bytes
	for i := nlen8; i < len(s); i++ {
		if s[i] >= 0x80 {
			return false
		}
	}
	return true
}
