package util

import (
	"hash/crc32"
	"io"
	"os"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tipb/go-binlog"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tidb/tablecodec"
)

var (
	mib = 1024 * 1024

	// SegmentSizeBytes is the max threshold of binlog segment file size
	// as an exported variable, you can define a different size
	SegmentSizeBytes int64 = 512 * 1024 * 1024

	magic uint32 = 471532804

	// ErrFileContentCorruption represents file or directory's content is curruption for some season
	ErrFileContentCorruption = errors.New("binlogger: content is corruption")

	// ErrCRCMismatch is the error represents crc don't match
	ErrCRCMismatch = errors.New("binlogger: crc mismatch")
	crcTable       = crc32.MakeTable(crc32.Castagnoli)

	// PrivateFileMode is the permission for service file
	PrivateFileMode os.FileMode = 0600
)

type binlogBuffer struct {
	cache []byte
}

var binlogBufferPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return &binlogBuffer{
			cache: make([]byte, mib),
		}
	},
}

// Walk reads binlog from the "from" position and sends binlogs in the streaming way
func Walk(filename string, tbInfo *model.TableInfo) error {
	var ent = &binlog.Entity{}
	var decoder Decoder

	f, err := os.OpenFile(filename, os.O_RDONLY, PrivateFileMode)
	if err != nil {
		return errors.Trace(err)
	}
	defer f.Close()

	from := binlog.Pos{
		Suffix: 0,
		Offset: 0,
	}
	decoder = NewDecoder(from, io.Reader(f))

	for {
		buf := binlogBufferPool.Get().(*binlogBuffer)
		err = decoder.Decode(ent, buf)
		if err != nil {
			break
		}

		b := new(binlog.Binlog)
		err = b.Unmarshal(ent.Payload)
		if err != nil {
			return errors.Trace(err)
		}
		preWriteValue := b.GetPrewriteValue()
		preWrite := &binlog.PrewriteValue{}
		err = preWrite.Unmarshal(preWriteValue)
		if err != nil {
			return errors.Errorf("prewrite %s unmarshal error %v", preWriteValue, err)
		}

		colsTypeMap := toColumnTypeMap(tbInfo.Columns)
		for _, mutation := range preWrite.Mutations {
			for _, row := range mutation.InsertedRows {
				remain, pk, err := codec.DecodeOne(row)
				if err != nil {
					log.Error("decode error")
					continue
				}
				columnValues, err := tablecodec.DecodeRow(remain, colsTypeMap, time.Local)
				if err != nil {
					log.Error("DecodeRow error")
					continue
				}
				columnValues[1] = pk

				log.Infof("[binlog] startTs: %d, commitTs: %d, id: %+v", b.StartTs, b.CommitTs, columnValues[1])
			}
		}
		/*
			newEnt := binlog.Entity{
				Pos:     ent.Pos,
				Payload: ent.Payload,
			}
		*/

		/*
			err := sendBinlog(newEnt)
			if err != nil {
				return errors.Trace(err)
			}
		*/

		binlogBufferPool.Put(buf)
	}

	if err != nil && err != io.EOF {
		return errors.Trace(err)
	}

	return nil
}

func toColumnTypeMap(columns []*model.ColumnInfo) map[int64]*types.FieldType {
	colTypeMap := make(map[int64]*types.FieldType)
	for _, col := range columns {
		colTypeMap[col.ID] = &col.FieldType
	}

	return colTypeMap
}