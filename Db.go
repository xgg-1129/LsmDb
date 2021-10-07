package LsmDb

import (
	"os"
)

const (
	DataBastPath ="D:\\Environment\\ProjectGo\\src\\LsmDb\\DbData"
	DataFileName = "data"
	MegerFileName ="Meger"
)

//一个数据库对应着一个文件
type Db struct {
	File *os.File
	//记录文件尾巴
	offset int64
}

func NewFile(fileName string)(*Db,error){
	//如果文件不存在则创建该文件
	file, err := os.OpenFile(fileName,os.O_CREATE|os.O_RDWR,0644)
	if err!=nil{
		return nil,err
	}
	fileStat, _ :=file.Stat()
	res:=&Db{
		File:   file,
		offset: fileStat.Size(),
	}
	return res,nil
}
func NewDataFile(path string)(*Db,error){
	filename:=path+string(os.PathSeparator)+DataFileName
	return NewFile(filename)
}
func NewMegerFile(path string)(*Db,error){
	filename:=path+string(os.PathSeparator)+MegerFileName
	return NewFile(filename)
}
func (d *Db) Read(offset int64)(e *Entry, err error) {
	buf := make([]byte, EntryHeadLen)
	if _, err = d.File.ReadAt(buf, offset); err != nil {
		return
	}
	e, err = DecoderHeader(buf)
	if err != nil {
		return
	}
	offset = offset + EntryHeadLen
	//读取key
	if e.keySize > 0 {
		keyBuf :=make([]byte,e.keySize)
		if _, err = d.File.ReadAt(keyBuf, offset);err!=nil{
			return
		}
		e.key= keyBuf
		offset=offset+(int64(e.keySize))
	}
	if e.valueSize > 0 {
		valueBuf:=make([]byte,e.valueSize)
		if _, err = d.File.ReadAt(valueBuf, offset);err!=nil{
			return
		}
		e.value=valueBuf
	}
	return

}

func (d *Db) Write(entry *Entry) error{
	//写入一个entry
	defer func() {
		d.offset =entry.GetSize()
	}()
	buf := entry.Encoder()

	_, err := d.File.WriteAt(buf, d.offset)
	if err!=nil {
		return err
	}
	return nil
}





