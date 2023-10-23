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


```agsl
DFCM 采用前一个 XOR 值对当前 XOR 值进行编码，具体编码规则如下：

第一个数值不用于压缩，用于作为参考值。
控制位计算规则：
如果 XOR 值为 0，存储 1bit：0
如果 XOR 值不为 0，控制位第一个 bit 存为 1，后续数据的计算方式如下：
如果当前 XOR 的 Meaningful bits 落在了前一个 XOR的 Meaningful bits 范围内（即当前 XOR 的 Leading Zeros 长度大于等于前一个 XOR 的 Leading Zeros 长度并且当前 XOR 的 Trailing Zeros 长度大于等于前一个 XOR 的 Trailing Zeros 长度），则控制位的第二个bit为 1，接下来存 XOR 非零的数值。


如果当前 XOR 的 Meaningful bits 不在前一个 XOR 的 Meaningful bits 范围内，则控制位的第二个 bit 为 0，后续存放：
5bits: Leading bits 个数
6bits: Meaningful bits 个数
XOR 非零的数值
```