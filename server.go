package objectstorage

import (
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
	serverGroup map[string]net.Conn
}

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
	s.serverGroup = make(map[string]net.Conn)
	for s.running {
		conn, err := ln.Accept()
		fmt.Println("Server connecting to foreign address " + conn.RemoteAddr().String())
		if err != nil {
			fmt.Println(err)
		}
		go s.handleConnection(conn)
	}
}

func (s *Server) Stop() {
	s.running = false
}

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

func (s *Server) handleMessage(msgValues []string, c net.Conn) int {
	msgType := msgValues[0]

	switch msgType {
	case PUT.String():
		key := msgValues[1]
		byteSize, _ := strconv.Atoi(msgValues[2])
		s.put(key, byteSize, c)
	case GET.String():
		key := msgValues[1]
		s.get(key, c)
	case DELETE.String():
		key := msgValues[1]
		s.delete(key, c)
	case LIST.String():
		s.list(c)
	case CONNECT.String():
		s.readConnMsg(msgValues[1], c)
	case DISCONNECT.String():
		return 1
	case ACKNOWLEDGE.String():
		writeAck(FAILURE, c)
	default:
		writeAck(FAILURE, c)
	}
	return 0
}

func (s *Server) readConnMsg(connType string, c net.Conn) {
	if connType == CLIENT.String() {
		if s.clientMax >= 5 {
			writeAck(FAILURE, c)
			c.Close()
			fmt.Println("Connection to address " + c.RemoteAddr().String() + " has been closed")
		}
		s.clientNum++
		writeAck(SUCCESS, c)
		fmt.Println("Connection to CLIENT " + c.RemoteAddr().String() + " is a Success")
	} else if connType == SERVER.String() {
		s.serverGroup[c.RemoteAddr().String()] = c
		writeAck(SUCCESS, c)
		fmt.Println("Connection to SERVER " + c.RemoteAddr().String() + " is a Success")
	}
	writeAck(FAILURE, c)
}

func (s *Server) put(key string, byteSize int, c net.Conn) {
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

func (s *Server) get(key string, c net.Conn) error {
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

	/*msgData, _ := bufio.NewReader(c).ReadString('\n')
	ackerr := handleAck(strings.TrimSpace(msgData), c)*/
	return nil
}

func (s *Server) delete(key string, c net.Conn) {
	s.dataStorage.Delete(key)
	writeAck(SUCCESS, c)
}

func (s *Server) list(c net.Conn) {
	writeAck(SUCCESS, c)

	var sb strings.Builder
	sb.WriteString(LIST.String() + "|")

	var keyList []string
	s.dataStorage.Range(func(key, value interface{}) bool {
		keyList = append(keyList, key.(string))
		return true
	})
	sb.WriteString(strconv.Itoa(len(keyList)))
	for _, key := range keyList {
		sb.WriteString("|" + key)
	}

	writeMsg(sb.String(), c)
}
