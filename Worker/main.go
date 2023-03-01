package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"time"
)

func main() {
	// lay id prossec
	pid := os.Getpid()
	StrPid := strconv.Itoa(pid)
	//ghi prossec ra file txt
	data := []byte(StrPid)
	err := ioutil.WriteFile("../ControllerWorker/Pid.txt", data, 0777)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(pid)
	time.Sleep(time.Hour)
}
