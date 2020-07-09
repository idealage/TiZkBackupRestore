# TiZkBackupRestore

这是一个zookeeper数据备份与恢复工具，支持权限信息的备份和恢复。

- 备份：将指定的结点（包括其下的所有子节点）备份至xml文件。

- 恢复：从xml文件将备份的数据恢复到指定结点。

##一、备份
```
python TiZkBackupRestore.py -t backup -p / -f zk_backup.xml --hosts 10.0.10.112:2181 -a "digest=jyp:123456" -d
```

##二、恢复
```
python TiZkBackupRestore.py -t restore -p / -f zk_backup.xml --hosts 10.0.10.112:2181 -a "digest=jyp:123456" -d
```

##三、节点复制

  此工具也可作为节点复制工具，示例如下：
```
# 将 node1 的数据复制到 node2
python TiZkBackupRestore.py -t backup -p /node1 -f zk_backup.xml --hosts 10.0.10.112:2181 -a "digest=jyp:123456" -d
python TiZkBackupRestore.py -t restore -p /node2 -f zk_backup.xml --hosts 10.0.10.112:2181 -a "digest=jyp:123456" -d
```

##四、参数说明
```
  -h    显示帮助
  -t    类型，backup=备份，restore=恢复
  -p    zookeeper 路径
  -f    备份或恢复的存储文件名
  -s    zookeeper 地址信息，例如：10.0.10.112:2181
  -a    权限信息, 支持多个权限，例如："digest=jyp:123456,digest=jyp2:123456"。（注意：需要使用双引号将内容括起来）
  -d    打印调试信息
```