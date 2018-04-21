package util

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"

	"github.com/pingcap/tipb/go-binlog"
)

// Decoder is an interface wraps basic Decode method which decode binlog.Entity into binlogBuffer.
type Decoder interface {
	Decode(ent *binlog.Entity, buf *binlogBuffer) error
}

type decoder struct {
	br  *bufio.Reader
	pos binlog.Pos
}

// NewDecoder creates a new Decoder.
func NewDecoder(pos binlog.Pos, r io.Reader) Decoder {
	reader := bufio.NewReader(r)

	return &decoder{
		br:  reader,
		pos: pos,
	}
}

// Decode implements the Decoder interface.
func (d *decoder) Decode(ent *binlog.Entity, buf *binlogBuffer) error {
	if d.br == nil {
		return io.EOF
	}

	// read and chekc magic number
	magicNum, err := readInt32(d.br)
	if err == io.EOF {
		d.br = nil
		return io.EOF
	}

	err = checkMagic(magicNum)
	if err != nil {
		return err
	}

	// read payload+crc  length
	size, err := readInt64(d.br)
	if err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	if len(buf.cache) < int(size+4) {
		buf.cache = make([]byte, size+4)
	}
	data := buf.cache[0 : size+4]

	// read payload+crc
	if _, err = io.ReadFull(d.br, data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}

	// decode bytes to ent struct and validate crc
	entryCrc := binary.LittleEndian.Uint32(data[size:])
	ent.Payload = data[:size]
	crc := crc32.Checksum(ent.Payload, crcTable)
	if crc != entryCrc {
		return ErrCRCMismatch
	}

	// 12 is size + magic length
	d.pos.Offset += size + 16

	ent.Pos = binlog.Pos{
		Suffix: d.pos.Suffix,
		Offset: d.pos.Offset,
	}

	return nil
}

func checkMagic(mgicNum uint32) error {
	if mgicNum != magic {
		return ErrCRCMismatch
	}

	return nil
}

func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}

func readInt32(r io.Reader) (uint32, error) {
	var n uint32
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
