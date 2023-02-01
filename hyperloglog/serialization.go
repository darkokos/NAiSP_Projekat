package hyperloglog

import (
	"bytes"
	"encoding/binary"
)

const (
	MAX_M = 65536 // Maksimalna duzina reg niza = 2 ^ HLL_MAX_PRECISION = 2 ^ 16
)

// Serijalizuje HyperLogLog u niz bajtova
func (hll *HLL) Serialize() []byte {
	serialized_bytes := &bytes.Buffer{}

	err := binary.Write(serialized_bytes, binary.LittleEndian, hll.m)
	if err != nil {
		return nil
	}

	err = binary.Write(serialized_bytes, binary.LittleEndian, hll.p)
	if err != nil {
		return nil
	}

	for _, reg := range hll.reg {
		err = binary.Write(serialized_bytes, binary.LittleEndian, reg)
		if err != nil {
			return nil
		}
	}

	err = binary.Write(serialized_bytes, binary.LittleEndian, hll.hashFunction.Seed)
	if err != nil {
		return nil
	}

	return serialized_bytes.Bytes()
}

// Deserijalizuje HyperLogLog iz niza bajtova
func DeserializeHLL(serialized_bytes []byte) *HLL {
	buf := bytes.NewBuffer(serialized_bytes)

	m_bytes := make([]byte, 8)
	err := binary.Read(buf, binary.LittleEndian, m_bytes)
	if err != nil {
		return nil
	}
	m := binary.LittleEndian.Uint64(m_bytes)

	p_bytes := make([]byte, 1)
	err = binary.Read(buf, binary.LittleEndian, p_bytes)
	if err != nil {
		return nil
	}
	p := uint8(p_bytes[0])

	if m > MAX_M {
		return nil // Procitana je nevalidna vrednost za m
	}
	reg := make([]uint8, m)
	for i := uint64(0); i < m; i++ {
		reg_byte := make([]byte, 1)
		err = binary.Read(buf, binary.LittleEndian, reg_byte)
		if err != nil {
			return nil
		}
		reg[i] = uint8(reg_byte[0])
	}

	seed := make([]byte, 32)
	err = binary.Read(buf, binary.LittleEndian, seed)
	if err != nil {
		return nil
	}

	hll := HLL{m: m, p: p, reg: reg, hashFunction: HashWithSeed{Seed: seed}}

	return &hll
}
