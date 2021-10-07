package LsmDb

import (
	"encoding/binary"
)

const EntryHeadLen = 10
type Entry struct {
	keySize   uint32
	valueSize uint32
	Mark      method
	key       []byte
	value     []byte

}
type method = uint16
const (
	Put  method=1
	Delete  method = 0
)

func CreateEntry(key []byte,value []byte,mark method) *Entry{
	entry:=&Entry{
		keySize:   0,
		valueSize: 0,
		key:       key,
		value:     value,
		Mark:      mark,
	}
	entry.keySize= uint32(len(key))
	entry.valueSize= uint32(len(value))
	return entry
}

func (e *Entry) GetSize() int64 {
	return int64(EntryHeadLen + e.valueSize + e.keySize)
}

func (e *Entry) Encoder()[]byte{
	buf:=make([]byte,e.GetSize())
	binary.BigEndian.PutUint32(buf[0:4], e.keySize)
	binary.BigEndian.PutUint32(buf[4:8], e.valueSize)
	binary.BigEndian.PutUint16(buf[8:10], e.Mark)
	copy(buf[EntryHeadLen:EntryHeadLen+e.keySize],e.key)
	copy(buf[EntryHeadLen+e.keySize:],e.value)
	return buf
}
func DecoderHeader(buf []byte)(*Entry,error) {
	ks:=binary.BigEndian.Uint32(buf[0:4])
	vs:=binary.BigEndian.Uint32(buf[4:8])
	mark:=binary.BigEndian.Uint16(buf[8:10])
	return &Entry{
		keySize:   ks,
		valueSize: vs,
		Mark:      mark,
	},nil
}
