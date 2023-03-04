package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

var MapPid = make(map[string]int)

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

func ReadPidWorker() int {
	time.Sleep(1000 * time.Millisecond)

	data, err := ioutil.ReadFile("Pid.txt")
	if err != nil {
		fmt.Println(err)
	}
	pid, _ := strconv.Atoi(string(data))
	return pid
}

func AddPidtoMap(itemkey string, pid int) {
	MapPid[itemkey] = pid
}

func OnWorker() error {
	cmnd := exec.Command("./worker")
	err := cmnd.Run()
	if err != nil {
		return err
	}
	return nil
}
func OffWorker(pid string) error {
	cmnd := exec.Command("kill", " "+pid)
	err := cmnd.Run()
	if err != nil {
		return err
	}
	return nil
}

func ReadMessagers() string {
	for {
		fmt.Println("hello read messages")
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{"localhost:9092"},
			Topic:    "on-or-off-Worker",
			MinBytes: 10e3,
			MaxBytes: 10e6,
		})
		r.SetOffset(kafka.LastOffset)
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			panic(err)
		}
		value := string(m.Value)
		return value
	}
}

func WriteMessages(topic, Messagers string) {
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

func OnOffWorker() error {
	messagers := ReadMessagers()
	state, itemkey := HandleString(messagers)
	pid := ReadPidWorker()
	if state == "on" {
		fmt.Println("Worker is running")
		go OnWorker()
		AddPidtoMap(itemkey, pid)
		fmt.Println(MapPid)
		// WriteMessages("onworker", "onworker")
	}
	if state == "off" {
		p := strconv.Itoa(pid)
		err := OffWorker(p)
		if err != nil {
			panic(err)
		}
		fmt.Println("worker off " + p)
	}
	return nil

}

func main() {
	for {
		OnOffWorker()
	}

}
