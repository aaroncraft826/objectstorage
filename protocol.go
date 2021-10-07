package objectstorage

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

type msgValue int64

const (
	PUT msgValue = iota
	GET
	DELETE
	LIST
	ACKNOWLEDGE
	SUCCESS
	FAILURE
	EXISTERROR
)

func (m msgValue) String() string {
	switch m {
	case PUT:
		return "PUT"
	case GET:
		return "GET"
	case DELETE:
		return "DEL"
	case LIST:
		return "LIS"
	case ACKNOWLEDGE:
		return "ACK"
	case SUCCESS:
		return "SUC"
	case FAILURE:
		return "FAI"
	case EXISTERROR:
		return "ER1"
	}
	return "UNKNOWN"
}

func writeMsg(msg string, c net.Conn) int {
	val, err := bufio.NewWriter(c).WriteString(msg)
	if err != nil {
		fmt.Println(err)
		return 1
	}

	msgData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return 1
	}
	handleAck(strings.TrimSpace(msgData), c)
	return 0
}

func writeAck(m msgValue, c net.Conn) int {
	val, err := bufio.NewWriter(c).WriteString(ACKNOWLEDGE.String() + "|" + m.String())
	if err != nil {
		fmt.Println(err)
		return 1
	}

	return 0
}

func handleAck(msg string, c net.Conn) int {
	msgValues := strings.Split(msg, "|")
	msgType := msgValues[0]
	ackVal := msgValues[1]

	if msgType != ACKNOWLEDGE.String() {
		fmt.Println("ERROR: Acknowledge message is not of type ACKNOWLEDGE")
		return 1
	}

	switch ackVal {
	case SUCCESS.String():
		return 0
	case FAILURE.String():
		fmt.Println("ERROR: Operation Failed")
		return 1
	default:
		fmt.Println("ERROR: Unknown response from Acknowledge message")
		return 1
	}
	return 1
}
