package objectstorage

import (
	"bufio"
	"errors"
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
	DISCONNECT
	ACKNOWLEDGE
	SUCCESS
	FAILURE
	EXISTERROR
	READERROR
	WRONGMSGERROR
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
	case DISCONNECT:
		return "DIS"
	case ACKNOWLEDGE:
		return "ACK"
	case SUCCESS:
		return "SUC"
	case FAILURE:
		return "FAI"
	case EXISTERROR:
		return "ER1"
	case READERROR:
		return "ER2"
	case WRONGMSGERROR:
		return "ER3"
	}
	return "UNKNOWN"
}

func writeMsg(msg string, c net.Conn) error {
	fmt.Println("Writing message: " + msg)
	writer := bufio.NewWriter(c)
	_, err := writer.WriteString(msg + "\n")
	if err != nil {
		fmt.Println(err)
		return err
	}
	writer.Flush()

	msgData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return err
	}

	return handleAck(strings.TrimSpace(msgData), c)
}

func writeAck(m msgValue, c net.Conn) error {
	writer := bufio.NewWriter(c)
	_, err := writer.WriteString(ACKNOWLEDGE.String() + "|" + m.String() + "\n")
	if err != nil {
		fmt.Println(err)
		return err
	}
	writer.Flush()

	return nil
}

func handleAck(msg string, c net.Conn) error {
	msgValues := strings.Split(msg, "|")
	msgType := msgValues[0]
	ackVal := msgValues[1]

	if msgType != ACKNOWLEDGE.String() {
		return errors.New("ERROR: Acknowledge message is not of type ACKNOWLEDGE")
	}

	switch ackVal {
	case SUCCESS.String():
		fmt.Println("Operation Success")
		return nil
	case FAILURE.String():
		return errors.New("ERROR: Operation Failed")
	default:
		return errors.New("ERROR: Unknown response from Acknowledge message")
	}
}

func readMsg(c net.Conn) ([]string, error) {
	msgData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return nil, err
	}
	fmt.Println("Message Read: " + strings.TrimSpace(msgData))
	return strings.Split(strings.TrimSpace(msgData), "|"), nil
}

func writeObj(obj []byte, c net.Conn) {
	writer := bufio.NewWriter(c)
	writer.Write(obj)
}

func readObj(obj []byte, c net.Conn) error {
	reader := bufio.NewReader(c)
	_, err := reader.Read(obj)
	return err
}
