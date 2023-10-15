## Block And Index
### Version 1:
```yaml
disk:
  position: 
    block:
      timestamps
      field-values

index:
  vin
  timestamp-positions
```
优点：
- 允许timestamp乱序，不存在更新的问题
- 不用考虑timestamp是否乱序
缺点：
- range查询时性能差，loop次数多，数据不连贯


block:
size 4bytes
timestamps 120 * 8bytes
positions 60 * 2bytes
columns values





### Version 2:
```yaml
disk:
  position:
    block:
      # timestamp-key = timestamp - timestamp % 2 * 60
      timestamp-key
      size
      field-values

index:
  vin
  timestamp-key-positions
```
优点：
- 数据有序，查询性能好
缺点：
- 写入时需要保证timestamp顺序


https://www.cnblogs.com/taosdata/p/16385388.html