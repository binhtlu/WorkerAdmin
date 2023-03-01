package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

var MapPid = make(map[string]int)

func HandlMessagers(wg *sync.WaitGroup) {
	for {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "worker",
			MinBytes: 10e3,
			MaxBytes: 10e6,
		})
		r.SetOffset(kafka.LastOffset)

		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(string(m.Value))
		cmnd, bskey := HandleString(string(m.Value))
		fmt.Println(MapPid[bskey])
		if cmnd == "close" {
			head := "kill"
			pid := strconv.Itoa(MapPid[bskey])
			state := killProcess(head, pid)
			if state == 1 {
				delete(MapPid, bskey)
				fmt.Println("Delete process success")
				WritePid()
			}
		}
		if cmnd == "open" && SetKey(bskey) == true {
			go OnWorker(wg)
			fmt.Println("Open worker")
			pid := ReadPidWorker()
			MapPid[bskey] = pid
			fmt.Println(MapPid)
			WritePid()
		}
		if err := r.Close(); err != nil {
			log.Fatal("failed to close reader:", err)
		}
	}
}

func OnWorker(wg *sync.WaitGroup) {
	cmnd := exec.Command("./worker")
	_, err := cmnd.Output()
	if err != nil {
		fmt.Println(err)
	}
}

func ReadPidWorker() int {
	time.Sleep(400 * time.Millisecond)

	data, err := ioutil.ReadFile("Pid.txt")
	if err != nil {
		fmt.Println(err)
	}
	pid, _ := strconv.Atoi(string(data))
	return pid
}

func killProcess(cmnd, pid string) int {
	key2 := " " + pid
	fmt.Println(key2)
	kill := exec.Command(cmnd, key2)
	err := kill.Run()
	if err != nil {
		return 0
	}
	return 1
}

func findString(s, stringFind string, stringCount int) bool {
	for i := 0; i < stringCount; i++ {
		if string(s[i]) == stringFind {
			return true
		}
	}
	return false
}

func HandleString(s string) (string, string) {
	a := []string{}
	b := []string{}

	s2 := strings.Split(s, ":")

	a = append(a, s2[0])
	b = append(b, s2[1])

	cmnd := strings.Join(a, "")
	key := strings.Join(b, "")

	LenOfcmnd := strings.Count(cmnd, "") - 1
	LenOfkey := strings.Count(key, "") - 1
	if findString(cmnd, " ", LenOfcmnd) == true {
		a1 := strings.Split(cmnd, " ")
		cmnd = strings.Join(a1, "")
	}
	if findString(key, " ", LenOfkey) == true {
		b1 := strings.Split(key, " ")
		key = strings.Join(b1, "")
	}
	return cmnd, key
}
func PrintPid(Wg *sync.WaitGroup) {
	fmt.Println(MapPid)
}

func SetKey(key string) bool {
	_, ok := MapPid[key]
	if key == "" {
		return false
	}
	if ok == true {
		return false
	}
	return true
}
func WritePid() {

	keys := make([]string, 0, len(MapPid))
	for k := range MapPid {
		keys = append(keys, k)
	}
	buf := &bytes.Buffer{}
	gob.NewEncoder(buf).Encode(keys)
	bs := buf.Bytes()
	fmt.Println(bs)
	err := ioutil.WriteFile("../ServiceWorker/MapPid.txt", bs, 0777)
	if err != nil {
		fmt.Println(err)
	}
}
func ReceiveMessagers(toppic, value string) bool {
	for {

		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    toppic,
			MinBytes: 10e3,
			MaxBytes: 10e6,
		})
		r.SetOffset(kafka.LastOffset)

		m, err := r.ReadMessage(context.Background())
		if err != nil {
			fmt.Println(err)
		}
		if string(m.Value) == value {
			return true
		}
		if string(m.Value) != value {
			return false
		}
	}

}
func SendMessagesr(topic string, Messagers string) {
	partition := 0
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(kafka.Message{Value: []byte(Messagers)})
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}
	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func ShowStateWoker(wg *sync.WaitGroup) {
	for {
		fmt.Println("hello show")
		mess := ReceiveMessagers("requetslistworker", "listworker")
		fmt.Println(mess)
		if mess == true {
			fmt.Println("vao")
			keys := make([]string, 0, len(MapPid))
			for k := range MapPid {
				keys = append(keys, k)
			}
			fmt.Println(keys)
			tostring := strings.Join(keys, "")
			fmt.Println(tostring)
			fmt.Println("chuan bi gui")
			SendMessagesr("responselistworker", tostring)
		}
	}
}

func ShowStateWoker2() {
	mess := ReceiveMessagers("requetslistworker2", "listworker")
	fmt.Println(mess)
	if mess == true {
		WritePid()
	}
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	var wg sync.WaitGroup
	wg.Add(4)
	go ShowStateWoker(&wg)
	go HandlMessagers(&wg)
	for {

		fmt.Println("command open worker (open : bskey)")
		fmt.Println("command close worker (close : pid)")
		fmt.Println("command quit programs ( q )")
		scanner.Scan()
		line := scanner.Text()
		countString := strings.Count(line, "") - 1
		if line == "mappid" {
			go PrintPid(&wg)
		}

		if findString(line, ":", countString) == true {
			cmnd, key := HandleString(line)
			fmt.Println(SetKey(key))
			if cmnd == "open" && SetKey(key) == true {
				go OnWorker(&wg)
				fmt.Println("Open worker")
				pid := ReadPidWorker()
				MapPid[key] = pid
				fmt.Println(MapPid)
				WritePid()
			}
			if cmnd == "close" {
				head := "kill"
				pid := strconv.Itoa(MapPid[key])
				state := killProcess(head, pid)
				if state == 1 {
					delete(MapPid, key)
					WritePid()
					fmt.Println("Delete process success")
				}
			}
		}
		if line == "q" {
			os.Exit(1)
		}
	}

}
