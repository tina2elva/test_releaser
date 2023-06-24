package main

import (
	"encoding/binary"
	"fmt"
	"math"
	"net"
	"strings"
	"time"
)

const (
	MAX_DURATION = 10000
)

type RealDB struct {
	conn net.Conn
}

func CreateRealDB(ip string, port int) (*RealDB, error) {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%v", ip, port))
	db := RealDB{
		conn: conn,
	}
	if err == nil {
		db.welcome()
	}

	return &db, err
}

func (this *RealDB) welcome() {
	buff := make([]byte, 512)
	this.conn.Read(buff)
}
func (this *RealDB) intToBytes(n int) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func (this *RealDB) bytesToInt(buf []byte) int {
	return int(binary.LittleEndian.Uint32(buf))
}

func (this *RealDB) byteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func (this *RealDB) float32ToBytes(n float32) []byte {
	bits := math.Float32bits(n)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)
	return bytes
}

func (this *RealDB) sendCMD(cmd, str string) {
	buff := make([]byte, 0)
	buff = append(buff, []byte(cmd)...)
	buff = append(buff, this.intToBytes(len(str))...)
	buff = append(buff, []byte(str)...)
	this.conn.Write(buff)
}

func (this *RealDB) Read(t time.Time, duration int, indexs ...int) ([]float32, error) {
	startTime := time.Date(2017, 1, 1, 0, 0, 0, 0, time.Local)
	timeStamp := int(t.Sub(startTime).Seconds())
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
		searchStr := fmt.Sprintf("%v %v %v", str, timeStamp, tmp)
		fmt.Println(searchStr)
		data, err := this.read(searchStr)
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

func (this *RealDB) read(str string) ([]float32, error) {
	this.sendCMD("read", str)
	buff := make([]byte, 512)
	result := make([]float32, 0)
	buffLen, err := this.conn.Read(buff)
	if err != nil {
		return result, err
	}
	//cmd := buff[0:4]
	lenArr := buff[4:8]
	length := this.bytesToInt(lenArr)
	buffIndex := 8
	totalIndex := 0
	for {
		valArr := buff[buffIndex : buffIndex+4]
		val := this.byteToFloat32(valArr)
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

func (this *RealDB) Write(indexs []int, values []float32) (int, error) {
	buff := make([]byte, 0)
	buff = append(buff, []byte("writ")...)
	buff = append(buff, this.intToBytes(len(values)*8)...)
	for i := 0; i < len(values); i++ {
		buff = append(buff, this.intToBytes(indexs[i])...)
		buff = append(buff, this.float32ToBytes(values[i])...)
	}
	return this.conn.Write(buff)
}

func (this *RealDB) Close() error {
	return this.conn.Close()
}
