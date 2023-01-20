package merkleTree

import (
	"crypto/sha1"
	"encoding/hex"
)

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

