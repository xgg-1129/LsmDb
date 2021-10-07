package LsmDb

import (
	"errors"
	"io"
	"os"
	"sync"
)

type DBMS struct {
	//当前管理的文件夹的目录
	Path string
	HashTable map[string]int64
	mu sync.RWMutex
	//数据文件
	Db *Db
}

func OpenDb(dirPath string)(dbms *DBMS,err error){
	//如果数据库文件夹不存在则建创建它
	if _, err = os.Stat(dirPath);os.IsNotExist(err){
		err = os.MkdirAll(dirPath,064)
		if err!=nil{
			return
		}
	}
	//加载数据库

	DataBase, err := NewDataFile(dirPath)
	if err != nil{
		return
	}
	dbms=&DBMS{}
	dbms.Db=DataBase
	dbms.Path=dirPath
	dbms.HashTable=make(map[string]int64)
	err = dbms.loadHashMap(DataBase)
	return
}

func (d *DBMS) loadHashMap(dataFile *Db)(error){


	 var offset int64 = 0
	 for {
		 entry, err := dataFile.Read(offset)
		 if err == nil {
		 	if err == io.EOF{
		 		break
			}
			return  err
		 }
		 d.HashTable[string(entry.key)]=offset
		 if entry.Mark== Delete{
		 	delete(d.HashTable,string(entry.key))
		 }
		 offset+=entry.GetSize()
	 }
	 return nil
}

func (d *DBMS) Put(key,value []byte)error{
	//禁读禁写
	d.mu.Lock()
	defer d.mu.Unlock()
	e:=CreateEntry(key,value,Put)
	tempOffset:=d.Db.offset
	err := d.Db.Write(e)
	if err!=nil{
		return err
	}
	//更新索引
	d.HashTable[string(key)]=tempOffset
	return nil
}

func (d *DBMS) Delete(key []byte)error{
	//禁读禁写
	d.mu.Lock()
	defer d.mu.Unlock()

	e:=CreateEntry(key,nil,Delete)
	delete(d.HashTable,string(key))
	return d.Db.Write(e)
}
func (d *DBMS) Get(key []byte)([]byte,error) {
	//可读禁写
	d.mu.RLock()
	defer d.mu.RUnlock()
	//如果索引不存在该key
	index,ok := d.HashTable[string(key)]
	if !ok{
		return nil,errors.New("can not find the key ")
	}
	entry, err := d.Db.Read(index)
	if err!=nil{
		return nil,err
	}
	if entry!=nil{
		return entry.value,nil
	}
	return nil,err
}
func (d *DBMS) Merge()(err error){
	//Merge的逻辑是在文件夹下创建一个meger文件，根据hashtabl,将有效的数据存进meger文件，最后在吧名字改掉
	if d.Db.offset == 0{
		return
	}
	//创建meger文件
	MegerFile, err := NewMegerFile(d.Path)

	if err!=nil{
		return err
	}
	for _,item := range d.HashTable{
		//读取有效entry
		entry ,_ := d.Db.Read(item)
		//entry放入新meger文件
		err = MegerFile.Write(entry)
		if err!=nil{
			return
		}
	}
	//更新索引
	if err = d.loadHashMap(MegerFile);err!=nil{
		return
	}
	//删除旧文件
	oldFileName:=d.Db.File.Name()
	if err = d.Db.File.Close();err!=nil{
		return
	}
	if err = os.Remove(oldFileName);err!=nil{
		return
	}

	//将Meger改为新的数据文件
	MegerName:=MegerFile.File.Name()

	if err = os.Rename(MegerName, d.Path+string(os.PathSeparator)+DataFileName);err!=nil{
		return
	}
	d.Db=MegerFile
	return

}


