package objectstorage

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

//Client is a struct that represents a client
type Client struct {
	addr      string
	port      int
	connected bool
	conn      net.Conn
}

//Connect connects to a server
func (c *Client) Connect(addr string, port int) error {
	servername := addr + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", servername)
	if err != nil {
		return err
	}

	//tell server that your a client
	err = writeMsg(CONNECT.String()+"|"+CLIENT.String(), conn)
	if err != nil {
		return err
	}

	c.conn = conn
	c.addr = addr
	c.port = port
	c.connected = true
	return nil
}

//Disconnect disconnects from the server
func (c *Client) Disconnect() error {
	writeMsg(DISCONNECT.String(), c.conn)
	c.conn.Close()
	c.connected = false
	return nil
}

//Put takes an object and puts its value into the key space (creates new key if key does not exist)
func (c *Client) Put(key string, obj []byte) error {
	writeMsg(PUT.String()+"|"+key+"|"+strconv.Itoa(len(obj)), c.conn)
	n, _ := c.conn.Write(obj)
	fmt.Println("Writing Object of Size " + strconv.Itoa(n))
	msgData, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return err
	}
	return handleAck(strings.TrimSpace(msgData), c.conn)
}

//gets the key's object from the server
func (c *Client) Get(key string) ([]byte, error) {
	writeMsg(GET.String()+"|"+key, c.conn)

	msgValues, _ := readMsg(c.conn)
	msgType := msgValues[0]
	byteSize, converr := strconv.Atoi(msgValues[1])
	if converr != nil {
		writeAck(FAILURE, c.conn)
		fmt.Println("FAILED TO CONVERT " + msgValues[1] + " TO INTEGER")
		return nil, converr
	}
	if msgType != GET.String() {
		writeAck(WRONGMSGERROR, c.conn)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + GET.String())
	}
	writeAck(SUCCESS, c.conn)

	var obj = make([]byte, byteSize)
	n, rerr := c.conn.Read(obj)
	fmt.Println("Reading Object of Size " + strconv.Itoa(n))

	if rerr != nil {
		//writeAck(READERROR, c.conn)
		return nil, rerr
	}
	//writeAck(SUCCESS, c.conn)
	return obj, nil
}

//deletes a key in the server
func (c *Client) Delete(key string) error {
	err := writeMsg(DELETE.String()+"|"+key, c.conn)
	return err
}

//Lists all objects in the server
func (c *Client) List() ([]string, error) {
	err := writeMsg(LIST.String(), c.conn)
	if err != nil {
		return nil, err
	}

	msgValues, rerr := readMsg(c.conn)
	if rerr != nil {
		return nil, rerr
	}
	msgType := msgValues[0]
	keyNum, _ := strconv.Atoi(msgValues[1])

	if msgType != LIST.String() {
		writeAck(WRONGMSGERROR, c.conn)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + LIST.String())
	}
	writeAck(SUCCESS, c.conn)

	keyList := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		keyList[i] = msgValues[i+2]
	}

	return keyList, nil
}
