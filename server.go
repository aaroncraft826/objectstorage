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
		go s.handleConnection(conn)
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

	serverList, err := s.GetServerList(hostConn)
	if err != nil {
		return nil
	}
	println("Server Group: ")
	println(strings.Join(serverList, ", "))

	for _, server := range serverList {
		if server != hostConn.LocalAddr().String() {
			s.connect(server)
		}
	}
	return nil
}

// connects to a foreign server and adds it to the caller server's serverGroup NOTE: ADDRESS MUST INCLUE :PORT
func (s *Server) connect(servername string) (net.Conn, error) {
	//servername := addr + ":" + strconv.Itoa(port)
	conn, err := net.Dial("tcp", servername)
	if err != nil {
		return nil, err
	}

	//tell server that your a Server
	err = writeMsg(CONNECT.String()+"|"+SERVER.String(), conn)
	if err != nil {
		return nil, err
	}

	s.serverGroup.Store(conn.RemoteAddr().String(), conn)
	go s.handleConnection(conn)
	return conn, nil
}

//gets list of a group's servers
func (s *Server) GetServerList(c net.Conn) ([]string, error) {
	err := writeMsg(LISTSERVERS.String(), c)
	if err != nil {
		return nil, err
	}

	msgValues, err := readMsg(c)
	if err != nil {
		return nil, err
	}

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
func (s *Server) handleConnection(c net.Conn) {
	fmt.Println("Connection to address " + c.RemoteAddr().String() + " is a Success")

	for s.running {
		msgValues, err := readMsg(c)
		if err != nil {
			fmt.Println(err)
			break
		}

		if s.handleMessage(msgValues, c) == 1 {
			fmt.Println("Connection to address " + c.RemoteAddr().String() + " has been closed")
			break
		}
	}

	c.Close()
}

//handles incoming messages in string[] form
func (s *Server) handleMessage(msgValues []string, c net.Conn) int {
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
		s.handleReadConnMsg(msgValues[1], c)
	case DISCONNECT.String():
		return 1
	case ACKNOWLEDGE.String():
		writeAck(FAILURE, c)
	default:
		writeAck(FAILURE, c)
	}
	return 0
}

//handles Connection messages
func (s *Server) handleReadConnMsg(connType string, c net.Conn) {
	if connType == CLIENT.String() {
		if s.clientMax >= 5 {
			writeAck(FAILURE, c)
			c.Close()
			fmt.Println("Connection to address " + c.RemoteAddr().String() + " has been closed")
			return
		}
		s.clientNum++
		writeAck(SUCCESS, c)
		fmt.Println("Connection to CLIENT " + c.RemoteAddr().String() + " is a Success")
		return
	} else if connType == SERVER.String() {
		s.serverGroup.Store(c.RemoteAddr().String(), c)
		//go s.handleConnection(c)
		writeAck(SUCCESS, c)
		fmt.Println("Connection to SERVER " + c.RemoteAddr().String() + " is a Success")
		return
	} else {
		writeAck(FAILURE, c)
	}
}

//handles Put messages
func (s *Server) handlePut(key string, byteSize int, c net.Conn) {
	writeAck(SUCCESS, c)
	var obj = make([]byte, byteSize)

	n, err := c.Read(obj)
	fmt.Println("Reading Object of Size " + strconv.Itoa(n))
	if err != nil {
		fmt.Println(err)
		writeAck(FAILURE, c)
		return
	}

	s.dataStorage.Store(key, obj)
	writeAck(SUCCESS, c)
}

//handles Get messages
func (s *Server) handleGet(key string, c net.Conn) error {
	obj, found := s.dataStorage.Load(key)
	if !found {
		writeAck(EXISTERROR, c)
		return errors.New("key already exists")
	}
	writeAck(SUCCESS, c)

	byteSize := len(obj.([]byte))
	writeMsg(GET.String()+"|"+strconv.Itoa(byteSize), c)
	n, _ := c.Write(obj.([]byte))
	fmt.Println("Writing Object of Size " + strconv.Itoa(n))

	return nil
}

//handles Delete messages
func (s *Server) handleDelete(key string, c net.Conn) {
	s.dataStorage.Delete(key)
	writeAck(SUCCESS, c)
}

//handles List messages
func (s *Server) handleList(c net.Conn, connType string) {
	if connType != SERVER.String() {
		writeAck(SUCCESS, c)
	}

	var sb strings.Builder
	sb.WriteString(LIST.String() + "|")

	var keyList []string
	s.dataStorage.Range(func(key, value interface{}) bool {
		keyList = append(keyList, key.(string))
		return true
	})

	if connType == CLIENT.String() {
		s.serverGroup.Range(func(key, value interface{}) bool {
			var sc = value.(net.Conn)
			skList, err := s.list(sc)
			fmt.Println("-------------SKLIST: " + strings.Join(skList, ", ") + "-----------------------")
			if err == nil {
				keyList = append(keyList, skList...)
			}
			return true
		})
	}

	sb.WriteString(strconv.Itoa(len(keyList)))
	for _, key := range keyList {
		sb.WriteString("|" + key)
	}

	writeMsg(sb.String(), c)
}

//handles list server messages
func (s *Server) handleServerList(c net.Conn) {
	writeAck(SUCCESS, c)

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

	writeMsg(sb.String(), c)
}

//put takes an object and puts its value into the key space of another server (creates new key if key does not exist)
func (s *Server) put(key string, obj []byte, c net.Conn) error {
	writeMsg(PUT.String()+"|"+key+"|"+strconv.Itoa(len(obj)), c)
	n, _ := c.Write(obj)
	fmt.Println("Writing Object of Size " + strconv.Itoa(n))
	msgData, err := bufio.NewReader(c).ReadString('\n')
	if err != nil {
		return err
	}
	return handleAck(strings.TrimSpace(msgData), c)
}

//gets the key's object from the server
func (s *Server) get(key string, c net.Conn) ([]byte, error) {
	writeMsg(GET.String()+"|"+key, c)

	msgValues, _ := readMsg(c)
	msgType := msgValues[0]
	byteSize, converr := strconv.Atoi(msgValues[1])
	if converr != nil {
		writeAck(FAILURE, c)
		fmt.Println("FAILED TO CONVERT " + msgValues[1] + " TO INTEGER")
		return nil, converr
	}
	if msgType != GET.String() {
		writeAck(WRONGMSGERROR, c)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + GET.String())
	}
	writeAck(SUCCESS, c)

	var obj = make([]byte, byteSize)
	n, rerr := c.Read(obj)
	fmt.Println("Reading Object of Size " + strconv.Itoa(n))

	if rerr != nil {
		//writeAck(READERROR, c.conn)
		return nil, rerr
	}
	//writeAck(SUCCESS, c.conn)
	return obj, nil
}

//deletes a key in the target server
func (s *Server) delete(key string, c net.Conn) error {
	err := writeMsg(DELETE.String()+"|"+key, c)
	return err
}

//Lists all objects in the target server
func (s *Server) list(c net.Conn) ([]string, error) {
	err := writeMsg(LIST.String()+"|"+SERVER.String(), c)
	if err != nil {
		return nil, err
	}

	msgValues, rerr := readMsg(c)
	if rerr != nil {
		return nil, rerr
	}
	msgType := msgValues[0]
	keyNum, _ := strconv.Atoi(msgValues[1])

	if msgType != LIST.String() {
		writeAck(WRONGMSGERROR, c)
		return nil, errors.New("Recieved message of type " + msgType + " instead of type " + LIST.String())
	}
	writeAck(SUCCESS, c)

	keyList := make([]string, keyNum)
	for i := 0; i < keyNum; i++ {
		keyList[i] = msgValues[i+2]
	}

	return keyList, nil
}
