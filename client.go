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
	conn      Connection
}

//Connect connects to a server
func (c *Client) Connect(addr string, port int) error {
	servername := addr + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", servername)
	if err != nil {
		return err
	}

	connStruct := Connection{netConn: conn, writer: bufio.NewWriter(conn), reader: bufio.NewReader(conn), remoteType: SERVER}

	//tell server that your a client
	err = connStruct.writeMsg(CONNECT.String() + "|" + CLIENT.String())
	if err != nil {
		return err
	}

	c.conn = connStruct
	c.addr = addr
	c.port = port
	c.connected = true
	return nil
}

//Disconnect disconnects from the server
func (c *Client) Disconnect() error {
	c.conn.writeMsg(DISCONNECT.String())
	c.conn.netConn.Close()
	c.connected = false
	return nil
}

//Put takes an object and puts its value into the key space (creates new key if key does not exist)
func (c *Client) Put(key string, obj []byte) error {
	c.conn.writeMsg(PUT.String() + "|" + key + "|" + strconv.Itoa(len(obj)))
	n, _ := c.conn.netConn.Write(obj)
	fmt.Println("Writing Object of Size " + strconv.Itoa(n))
	msgData, err := c.conn.reader.ReadString('\n')
	if err != nil {
		return err
	}
	return c.conn.handleAck(strings.TrimSpace(msgData))
}

//gets the key's object from the server
func (c *Client) Get(key string) ([]byte, error) {
	c.conn.writeMsg(GET.String() + "|" + key)

	msgValues, _ := c.conn.readMsg()
	msgType := msgValues[0]
	byteSize, converr := strconv.Atoi(msgValues[1])
	if converr != nil {
		c.conn.writeAck(FAILURE)
		fmt.Println("FAILED TO CONVERT " + msgValues[1] + " TO INTEGER")
		return nil, converr
	}
	if msgType != GET.String() {
		c.conn.writeAck(WRONGMSGERROR)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + GET.String())
	}
	c.conn.writeAck(SUCCESS)

	var obj = make([]byte, byteSize)
	n, rerr := c.conn.reader.Read(obj)
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
	err := c.conn.writeMsg(DELETE.String() + "|" + key)
	return err
}

//Lists all objects in the server
func (c *Client) List() ([]string, error) {
	err := c.conn.writeMsg(LIST.String() + "|" + CLIENT.String())
	if err != nil {
		return nil, err
	}

	msgValues, rerr := c.conn.readMsg()
	if rerr != nil {
		return nil, rerr
	}
	msgType := msgValues[0]
	keyNum, _ := strconv.Atoi(msgValues[1])

	if msgType != LIST.String() {
		c.conn.writeAck(WRONGMSGERROR)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + LIST.String())
	}
	c.conn.writeAck(SUCCESS)

	keyList := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		keyList[i] = msgValues[i+2]
	}

	return keyList, nil
}
