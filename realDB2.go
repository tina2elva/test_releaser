package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"
)

type RealDB2 struct {
	conn net.Conn
}

func CreateRealDB2(ip string, port int) (*RealDB2, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	db := RealDB2{
		conn: conn,
	}
	if err == nil {
		db.welcome()
	}

	return &db, err
}

func (this *RealDB2) welcome() {
	buff := make([]byte, 512)
	this.conn.Read(buff)
}

func (this *RealDB2) longToBytes(n int64) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
		byte(n >> 32),
		byte(n >> 40),
		byte(n >> 48),
		byte(n >> 56),
	}
}

func (this *RealDB2) bytesToLong(buf []byte) int64 {
	return int64(binary.LittleEndian.Uint64(buf))
}

func (this *RealDB2) shortToBytes(n uint16) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
	}
}

func (this *RealDB2) bytesToShort(buf []byte) uint16 {
	return binary.LittleEndian.Uint16(buf)
}

func (this *RealDB2) intToBytes(n int) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func (this *RealDB2) bytesToInt(buf []byte) int {
	return int(binary.LittleEndian.Uint32(buf))
}

func (this *RealDB2) byteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func (this *RealDB2) float32ToBytes(n float32) []byte {
	bits := math.Float32bits(n)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}

func (this *RealDB2) sendReadCMD(timeStamp, duration int, indexs []uint16) {
	// read，请求格式
	// read|body长度|8位随机码|4位开始时间戳|4位duration|读取地址ushort数组）
	buff := make([]byte, 0)
	buff = append(buff, []byte("read")...)
	buff = append(buff, this.intToBytes(8+4+4+2*len(indexs))...)
	buff = append(buff, this.longToBytes(1)...)
	buff = append(buff, this.intToBytes(timeStamp)...)
	buff = append(buff, this.intToBytes(duration)...)
	for i := 0; i < len(indexs); i++ {
		buff = append(buff, this.shortToBytes(indexs[i])...)
	}
	this.conn.Write(buff)
}

func (this *RealDB2) parseResult(buff []byte) {
	// read，返回格式
	// read|body长度|8位随机码|1为返回码|结果
	// write，返回格式
	// writ|body长度|8位随机码|1为返回码|
	cmd := string(buff[0:4])
	//length := buff[4:8]
	//token := buff[8:16]
	//code := buff[16:17]
	switch cmd {
	case "read":
		break
	case "writ":
		break
	default:
		break
	}
}

func (this *RealDB2) sendWriteCMD(timeStamp int, indexs []uint16, values []float32) {
	// write，请求格式
	// writ|body长度|8位随机码|4位时间戳|写入数值数组（两位地址(ushort)+四位数据）
	buff := make([]byte, 0)
	buff = append(buff, []byte("writ")...)
	buff = append(buff, this.intToBytes(8+4+(2+4)*len(indexs))...)
	buff = append(buff, this.longToBytes(1)...)
	buff = append(buff, this.intToBytes(timeStamp)...)
	for i := 0; i < len(indexs); i++ {
		buff = append(buff, this.shortToBytes(indexs[i])...)
		buff = append(buff, this.float32ToBytes(values[i])...)
	}
	this.conn.Write(buff)
}

func (this *RealDB2) sendCMD(cmd, str string) {
	buff := make([]byte, 0)
	buff = append(buff, []byte(cmd)...)
	buff = append(buff, this.intToBytes(len(str))...)
	buff = append(buff, []byte(str)...)
	this.conn.Write(buff)
}

func (this *RealDB2) Read(t time.Time, duration int, indexs ...uint16) ([]float32, error) {

	// edit by xuyb 2021-10-29
	// 基准时间必须为UTC时间的2017-01-01
	startTime := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	//fmt.Println(t, t.UTC(), startTime)
	// timeStamp := int(t.UTC().Sub(startTime).Seconds())
	timeStamp := int(t.Unix() - startTime.Unix())
	//fmt.Println(timeStamp, t.Unix()-time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local).Unix())
	str := fmt.Sprintf("%v", indexs)

	str = strings.ReplaceAll(str, " ", ",")
	result := make([][]float32, len(indexs))
	for {
		tmp := MAX_DURATION
		if duration > MAX_DURATION {
			tmp = MAX_DURATION
			duration = duration - tmp
		} else {
			tmp = duration
			duration = 0
		}
		//searchStr := fmt.Sprintf("%v %v %v", str, timeStamp, tmp)
		//fmt.Println(searchStr)
		data, err := this.read(timeStamp, tmp, indexs)
		if err != nil {
			return nil, err
		}
		for i := 0; i < len(indexs); i++ {
			result[i] = append(result[i], data[i*tmp:(i+1)*tmp]...)
		}
		if duration == 0 {
			break
		}
		timeStamp = timeStamp + tmp
	}
	ret := make([]float32, 0)
	for i := 0; i < len(indexs); i++ {
		ret = append(ret, result[i]...)
	}
	return ret, nil
}

func (this *RealDB2) handleResHeader() (int, error) {
	buff := make([]byte, 17)
	_, err := this.conn.Read(buff)
	if err != nil {
		return -1, err
	}
	//cmd := buff[0:4]
	lenArr := buff[4:8]
	length := this.bytesToInt(lenArr)
	//rndArr := buff[8:16]
	//rnd := this.bytesToLong(rndArr)
	//fmt.Println("rnd", rnd)
	flag := buff[16]
	if flag == 1 {
		return length, nil
	}
	return -1, fmt.Errorf("Error: %v", flag)
}

func (this *RealDB2) read(timeStamp, duration int, indexs []uint16) ([]float32, error) {
	this.sendReadCMD(timeStamp, duration, indexs)
	result := make([]float32, 0)
	length, err := this.handleResHeader()
	if err != nil {
		return result, err
	}
	length = length - 9 // 减去随机数长度和结果标识长度
	buffIndex := 0
	totalIndex := 0

	buff := make([]byte, 512)
	buffLen, err := this.conn.Read(buff)
	if err != nil {
		return result, err
	}
	for {
		valArr := buff[buffIndex : buffIndex+4]
		val := this.byteToFloat32(valArr)
		fmt.Println(val)
		if math.IsNaN(float64(val)) {
			val = 0
		}
		result = append(result, val)
		buffIndex = buffIndex + 4
		totalIndex = totalIndex + 4
		if totalIndex >= length {
			break
		}
		if buffIndex >= buffLen {
			buffIndex = 0
			buff = make([]byte, 512)
			buffLen, err = this.conn.Read(buff)
			if err != nil {
				return result, err
			}
		}
	}
	return result, nil
}

func (this *RealDB2) Write(indexs []uint16, values []float32) (int, error) {
	this.sendWriteCMD(int(time.Now().Unix()), indexs, values)
	return this.handleResHeader()
}

func (this *RealDB2) Close() error {
	return this.conn.Close()
}
