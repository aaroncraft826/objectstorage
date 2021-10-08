package objectstorage

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"strings"
)

type Client struct {
	addr      string
	port      int
	connected bool
	conn      net.Conn
}

func (c *Client) Connect(addr string, port int) error {
	servername := addr + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", servername)
	if err != nil {
		return errors.New("Could not connect to server " + servername)
	}
	c.conn = conn
	c.addr = addr
	c.port = port
	c.connected = true
	return nil
}

func (c *Client) Disconnect() error {
	writeMsg(DISCONNECT.String(), c.conn)
	c.conn.Close()
	c.connected = false
	return nil
}

func (c *Client) Put(key string, obj []byte) error {
	writeMsg(PUT.String()+"|"+key+"|"+strconv.Itoa(len(obj)), c.conn)
	c.conn.Write(obj)
	msgData, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		return err
	}
	return handleAck(strings.TrimSpace(msgData), c.conn)
}

func (c *Client) Get(key string) ([]byte, error) {
	writeMsg(GET.String()+"|"+key, c.conn)

	msgValues, _ := readMsg(c.conn)
	msgType := msgValues[0]
	byteSize, _ := strconv.Atoi(msgValues[1])
	if msgType != GET.String() {
		writeAck(WRONGMSGERROR, c.conn)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + GET.String())
	}

	var obj = make([]byte, byteSize)
	val, rerr := c.conn.Read(obj)
	_ = val

	if rerr != nil {
		writeAck(READERROR, c.conn)
		return nil, rerr
	}
	writeAck(SUCCESS, c.conn)
	return obj, nil
}

func (c *Client) Delete(key string) error {
	err := writeMsg(DELETE.String()+"|"+key, c.conn)
	return err
}

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

	keyList := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		keyList[i] = msgValues[i+2]
	}

	return keyList, nil
}
