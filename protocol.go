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
	LISTSERVERS
	CONNECT
	SERVER
	CLIENT
	DISCONNECT
	ACKNOWLEDGE
	SUCCESS
	FAILURE
	DUMMY
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
	case LISTSERVERS:
		return "LSE"
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
	case DUMMY:
		return "DUM"
	case EXISTERROR:
		return "ER1"
	case READERROR:
		return "ER2"
	case WRONGMSGERROR:
		return "ER3"
	}
	return "UNKNOWN"
}

//wrapper for net.Conn, so that we don't need to rewrite bufio writer/reader
//, It is vital that netConn, writer and reader are all initialized
type Connection struct {
	netConn    net.Conn
	writer     *bufio.Writer
	reader     *bufio.Reader
	remoteType msgValue
}

//writeMsg() a message string to the connection, then waits for an acknowledgement
func (c *Connection) writeMsg(msg string) error {
	fmt.Println("Writing message: " + msg)
	_, err := c.writer.WriteString(msg + "\n")
	if err != nil {
		fmt.Println(err)
		return err
	}
	c.writer.Flush()

	msgData, err := c.reader.ReadString('\n')
	if err != nil {
		fmt.Println(err)
		return err
	}

	return c.handleAck(strings.TrimSpace(msgData))
}

//writeAck() takes a msgValue (usually SUCCESS or FAILURE) and sends out an acknowledgent with that value
func (c *Connection) writeAck(m msgValue) error {
	_, err := c.writer.WriteString(ACKNOWLEDGE.String() + "|" + m.String() + "\n")
	if err != nil {
		fmt.Println(err)
		return err
	}
	c.writer.Flush()

	return nil
}

//reads an acknowledgement string
func (c *Connection) handleAck(msg string) error {
	msgValues := strings.Split(msg, "|")
	for msgValues[0] == DUMMY.String() {
		msgData, _ := c.reader.ReadString('\n')
		msgValues = strings.Split(strings.TrimSpace(msgData), "|")
	}
	msgType := msgValues[0]
	ackVal := msgValues[1]

	if msgType != ACKNOWLEDGE.String() {
		return errors.New("ERROR: Acknowledge message of type " + msgType + " instead of type ACKNOWLEDGE")
	}

	switch ackVal {
	case SUCCESS.String():
		fmt.Println("Operation Success")
		return nil
	case FAILURE.String():
		return errors.New("ERROR: Operation Failed")
	case EXISTERROR.String():
		return errors.New("ERROR: Existence Error")
	default:
		return errors.New("ERROR: Unknown response from Acknowledge message")
	}
}

//reads a message string and tokenizes it into values (DOES NOT SEND ACKNOWLEDGEMENT)
func (c *Connection) readMsg() ([]string, error) {
	msgData, err := c.reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	fmt.Println("Message Read: " + strings.TrimSpace(msgData))

	var output = strings.Split(strings.TrimSpace(msgData), "|")
	/*if output[0] == ACKNOWLEDGE.String() {
		return output, errors.New("read Message of type acknowledge")
	}*/
	return output, nil
}

//Writes a dummy message to bypass read's, writes DUMMY and next line BUT DOESN'T FLUSH
func (c *Connection) writeDummy() error {
	fmt.Println("Writing Dummy")
	_, err := c.writer.WriteString(DUMMY.String() + "\n")
	if err != nil {
		fmt.Println(err)
		return err
	}
	//c.writer.Flush()
	return nil
}
