###LsmDb是一个基于bitcask数据存储类型实现的的kv数据存储引擎，数据的插入、更新、删除都会被记录成一条日志，然后追加写入到磁盘文件当中


1.这里追加的记录叫做Entry,Entry里存放了key、value的大小，以及这条记录的操作是Delete还是Put

2.LsmDb运行的时候，会在内存中维护一个哈希表，这个哈希表作为所有有效数据的索引

3.Merge操作会生成一个临时文件，然后读取数据文件上的有效数据，再删除数据文件，临时文件会成为新的数据文件，Meger能够硬盘中的无效数据
