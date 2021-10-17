package objectstorage

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

//Server is a struct that represents a server
type Server struct {
	port        int
	clientMax   int
	clientNum   int
	running     bool
	dataStorage sync.Map
	serverGroup sync.Map
}

//starts server NOTE: If using multiple servers on the same main method, you may want to call this method on a goroutine
func (s *Server) Start(port int) {
	if s.running {
		fmt.Println("ERROR: Server already running")
		return
	}

	s.port = port
	s.running = true
	ln, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	if err != nil {
		fmt.Println(err)
		return
	}

	s.dataStorage = sync.Map{}
	s.serverGroup = sync.Map{}
	for s.running {
		conn, err := ln.Accept()
		fmt.Println("Server connecting to foreign address " + conn.RemoteAddr().String())
		if err != nil {
			fmt.Println(err)
		}
		go s.handleConnection(Connection{netConn: conn, writer: bufio.NewWriter(conn), reader: bufio.NewReader(conn)})
	}
}

//stops server
func (s *Server) Stop() {
	s.running = false
}

//connects to a foreign server's group
func (s *Server) Connect(addr string, port int) error {

	hostName := addr + ":" + strconv.Itoa(port)
	hostConn, err := s.connect(hostName)
	if err != nil {
		return nil
	}

	serverList, err := s.GetServerList(*hostConn)
	if err != nil {
		return err
	}
	println("Server Group: ")
	println(strings.Join(serverList, ", "))

	for _, server := range serverList {
		if server != hostConn.netConn.LocalAddr().String() {
			s.connect(server)
		}
	}
	return nil
}

// connects to a foreign server and adds it to the caller server's serverGroup NOTE: ADDRESS MUST INCLUE :PORT
func (s *Server) connect(servername string) (*Connection, error) {
	//servername := addr + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", servername)
	if err != nil {
		return nil, err
	}
	connStruct := Connection{netConn: conn, writer: bufio.NewWriter(conn), reader: bufio.NewReader(conn), remoteType: SERVER}

	//tell server that your a Server
	err = connStruct.writeMsg(CONNECT.String() + "|" + SERVER.String())
	if err != nil {
		return nil, err
	}

	s.serverGroup.Store(conn.RemoteAddr().String(), connStruct)
	go s.handleConnection(connStruct)
	return &connStruct, nil
}

//gets list of a group's servers
func (s *Server) GetServerList(c Connection) ([]string, error) {
	err := c.writeMsg(LISTSERVERS.String())
	if err != nil {
		return nil, err
	}

	msgValues, err := c.readMsg()
	if err != nil {
		c.writeAck(FAILURE)
		return nil, err
	}
	c.writeAck(SUCCESS)

	size, err := strconv.Atoi(msgValues[1])
	if err != nil {
		return nil, err
	}

	var output []string = make([]string, size)
	for i := 2; i < len(msgValues); i++ {
		output[i-2] = msgValues[i]
	}
	return output, nil
}

//handles connections
func (s *Server) handleConnection(c Connection) {
	fmt.Println("Connection to address " + c.netConn.RemoteAddr().String() + " is a now being handled")

	for s.running {
		msgValues, err := c.readMsg()
		if err != nil {
			println("ITS AT HANDLECONNECTION " + err.Error())
			fmt.Println(err.Error())
			break
		}

		if s.handleMessage(msgValues, c) == 1 {
			break
		}
	}

	fmt.Println("Connection to address " + c.netConn.RemoteAddr().String() + " has been closed")
	c.netConn.Close()
}

//handles incoming messages in string[] form
func (s *Server) handleMessage(msgValues []string, c Connection) int {
	msgType := msgValues[0]

	switch msgType {
	case PUT.String():
		key := msgValues[1]
		byteSize, _ := strconv.Atoi(msgValues[2])
		s.handlePut(key, byteSize, c)
	case GET.String():
		key := msgValues[1]
		s.handleGet(key, c)
	case DELETE.String():
		key := msgValues[1]
		s.handleDelete(key, c)
	case LIST.String():
		connType := msgValues[1]
		s.handleList(c, connType)
	case LISTSERVERS.String():
		s.handleServerList(c)
	case CONNECT.String():
		s.handleReadConnMsg(msgValues[1], &c)
	case DUMMY.String():
		return 0
	case DISCONNECT.String():
		return 1
	case ACKNOWLEDGE.String():
		c.writeAck(FAILURE)
	default:
		c.writeAck(FAILURE)
	}
	return 0
}

//handles Connection messages
func (s *Server) handleReadConnMsg(connType string, c *Connection) {
	if connType == CLIENT.String() {
		if s.clientMax >= 5 {
			c.writeAck(FAILURE)
			c.netConn.Close()
			fmt.Println("Connection to address " + c.netConn.RemoteAddr().String() + " has been closed")
			return
		}
		c.remoteType = CLIENT
		s.clientNum++
		c.writeAck(SUCCESS)
		fmt.Println("Connection to CLIENT " + c.netConn.RemoteAddr().String() + " is a Success")
		return
	} else if connType == SERVER.String() {
		c.remoteType = SERVER
		s.serverGroup.Store(c.netConn.RemoteAddr().String(), c)
		c.writeAck(SUCCESS)
		fmt.Println("Connection to SERVER " + c.netConn.RemoteAddr().String() + " is a Success")
		return
	} else {
		c.writeAck(FAILURE)
	}
}

//handles Put messages
func (s *Server) handlePut(key string, byteSize int, c Connection) {
	c.writeAck(SUCCESS)
	var obj = make([]byte, byteSize)

	n, err := c.netConn.Read(obj)
	fmt.Println("Reading Object of Size " + strconv.Itoa(n))
	if err != nil {
		fmt.Println(err)
		c.writeAck(FAILURE)
		return
	}

	s.dataStorage.Store(key, obj)
	c.writeAck(SUCCESS)
}

//handles Get messages
func (s *Server) handleGet(key string, c Connection) error {
	obj, found := s.dataStorage.Load(key)
	if !found {
		c.writeAck(EXISTERROR)
		return errors.New("key already exists")
	}
	c.writeAck(SUCCESS)

	byteSize := len(obj.([]byte))
	c.writeMsg(GET.String() + "|" + strconv.Itoa(byteSize))
	n, _ := c.writer.Write(obj.([]byte))
	fmt.Println("Writing Object of Size " + strconv.Itoa(n))

	return nil
}

//handles Delete messages
func (s *Server) handleDelete(key string, c Connection) {
	s.dataStorage.Delete(key)
	c.writeAck(SUCCESS)
}

//handles List messages
func (s *Server) handleList(c Connection, connType string) {
	if connType == SERVER.String() {
		c.writeDummy()
	}
	c.writeAck(SUCCESS)

	var sb strings.Builder
	sb.WriteString(LIST.String() + "|")

	var keyList []string
	s.dataStorage.Range(func(key, value interface{}) bool {
		keyList = append(keyList, key.(string))
		return true
	})

	if connType == CLIENT.String() {
		s.serverGroup.Range(func(key, value interface{}) bool {
			var sc = value.(Connection)
			skList, err := s.list(sc)
			fmt.Println("-------------SKLIST: " + strings.Join(skList, ", ") + "-----------------------")
			if err == nil {
				keyList = append(keyList, skList...)
			} else {
				println("---------------------" + err.Error() + "----------------------")
			}
			return true
		})
	}

	sb.WriteString(strconv.Itoa(len(keyList)))
	for _, key := range keyList {
		sb.WriteString("|" + key)
	}

	c.writeMsg(sb.String())
}

//handles list server messages
func (s *Server) handleServerList(c Connection) {
	c.writeDummy()
	c.writeAck(SUCCESS)

	var sb strings.Builder
	sb.WriteString(LISTSERVERS.String() + "|")

	var serverList []string
	s.serverGroup.Range(func(server, value interface{}) bool {
		serverList = append(serverList, server.(string))
		return true
	})
	sb.WriteString(strconv.Itoa(len(serverList)))
	for _, key := range serverList {
		sb.WriteString("|" + key)
	}

	c.writeMsg(sb.String())
}

//put takes an object and puts its value into the key space of another server (creates new key if key does not exist)
func (s *Server) put(key string, obj []byte, c Connection) error {
	c.writeMsg(PUT.String() + "|" + key + "|" + strconv.Itoa(len(obj)))
	n, _ := c.writer.Write(obj)
	fmt.Println("Writing Object of Size " + strconv.Itoa(n))
	msgData, err := c.reader.ReadString('\n')
	if err != nil {
		return err
	}
	return c.handleAck(strings.TrimSpace(msgData))
}

//gets the key's object from the server
func (s *Server) get(key string, c Connection) ([]byte, error) {
	c.writeMsg(GET.String() + "|" + key)

	msgValues, _ := c.readMsg()
	msgType := msgValues[0]
	byteSize, converr := strconv.Atoi(msgValues[1])
	if converr != nil {
		c.writeAck(FAILURE)
		fmt.Println("FAILED TO CONVERT " + msgValues[1] + " TO INTEGER")
		return nil, converr
	}
	if msgType != GET.String() {
		c.writeAck(WRONGMSGERROR)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + GET.String())
	}
	c.writeAck(SUCCESS)

	var obj = make([]byte, byteSize)
	n, rerr := c.reader.Read(obj)
	fmt.Println("Reading Object of Size " + strconv.Itoa(n))

	if rerr != nil {
		//writeAck(READERROR, c.conn)
		return nil, rerr
	}
	//writeAck(SUCCESS, c.conn)
	return obj, nil
}

//deletes a key in the target server
func (s *Server) delete(key string, c Connection) error {
	err := c.writeMsg(DELETE.String() + "|" + key)
	return err
}

//Lists all objects in the target server
func (s *Server) list(c Connection) ([]string, error) {
	err := c.writeMsg(LIST.String() + "|" + SERVER.String())
	if err != nil {
		return nil, err
	}

	msgValues, rerr := c.readMsg()
	if rerr != nil {
		return nil, rerr
	}
	msgType := msgValues[0]
	keyNum, _ := strconv.Atoi(msgValues[1])

	if msgType != LIST.String() {
		c.writeAck(WRONGMSGERROR)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + LIST.String())
	}
	c.writeAck(SUCCESS)

	keyList := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		keyList[i] = msgValues[i+2]
	}

	return keyList, nil
}
