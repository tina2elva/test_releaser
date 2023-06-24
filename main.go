package main

import (
	"encoding/binary"
	// "encoding/csv"
	"errors"
	"fmt"
	"math"

	// "os"
	"os/exec"
	"strings"
	"time"
)

func Int32ToBytes(n int) []byte {
	return []byte{
		byte(n),
		byte(n >> 8),
		byte(n >> 16),
		byte(n >> 24),
	}
}

func BytesToInt32(buf []byte) int {
	return int(buf[0] | buf[1]<<8 | buf[2]<<16 | buf[3]<<24)
}

func ByteToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)
	return math.Float32frombits(bits)
}

func main() {

	c := make(chan int)
	db, err := CreateRealDB2("139.9.233.66", 3001)
	fmt.Println(db, err)
	defer db.Close()
	ret, err := db.Read(time.Date(2023, 5, 8, 12, 0, 0, 0, time.Local), 1000, 11)
	fmt.Println("read", len(ret), ret, err)
	ret, err = db.Read(time.Date(2022, 2, 22, 22, 0, 0, 0, time.Local), 10, 0, 1, 2, 3)
	fmt.Println(len(ret), ret, err)
	//fmt.Println(db.Write([]int{22}, []float32{33}))
	<-c
	// CmdPythonSaveImageDpi()
}

//执行python脚本
func CmdPythonSaveImageDpi() (err error) {
	args := []string{"C:\\taiji\\OPCPy\\main.py"}
	out, err := exec.Command("C:\\ProgramData\\Anaconda3\\envs\\OPCPy\\python.exe", args...).Output()
	if err != nil {
		fmt.Println(out, err)
		return
	}
	result := string(out)
	if strings.Index(result, "success") != 0 {
		err = errors.New(fmt.Sprintf("main.py error：%s", result))
	}
	return
}
