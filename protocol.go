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
	CONNECT
	SERVER
	CLIENT
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
	case CONNECT:
		return "CON"
	case SERVER:
		return "SER"
	case CLIENT:
		return "CLI"
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

//writeMsg() a message string to the connection, then waits for an acknowledgement
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

//writeAck() takes a msgValue (usually SUCCESS or FAILURE) and sends out an acknowledgent with that value
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

//reads an acknowledgement string
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

//reads a message string and tokenizes it into values (DOES NOT SEND ACKNOWLEDGEMENT)
func readMsg(c net.Conn) ([]string, error) {
	msgData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return nil, err
	}
	fmt.Println("Message Read: " + strings.TrimSpace(msgData))
	return strings.Split(strings.TrimSpace(msgData), "|"), nil
}
