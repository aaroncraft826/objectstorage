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

type Server struct {
	port        int
	clientMax   int
	clientNum   int
	running     bool
	dataStorage sync.Map
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
	for s.running {
		conn, err := ln.Accept()
		fmt.Println("Server connecting to client " + conn.RemoteAddr().Network())
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
	if s.clientMax >= 5 {
		c.Close()
	}
	s.clientNum++

	for s.running {
		msgData, err := bufio.NewReader(c).ReadString('\n')
		if err != nil {
			fmt.Println(err)
			break
		}
		fmt.Println("Message Recieved: " + strings.TrimSpace(msgData))
		if s.handleMessage(strings.TrimSpace(msgData), c) == 1 {
			fmt.Println("Connection " + c.RemoteAddr().String() + " has been closed")
			break
		}
	}

	c.Close()
}

func (s *Server) handleMessage(msg string, c net.Conn) int {
	msgValues := strings.Split(msg, "|")
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
	case DISCONNECT.String():
		return 1
	case ACKNOWLEDGE.String():
		writeAck(FAILURE, c)
	default:
		writeAck(FAILURE, c)
	}
	return 0
}

func (s *Server) put(key string, byteSize int, c net.Conn) {
	writeAck(SUCCESS, c)
	var obj = make([]byte, byteSize)

	_, err := c.Read(obj)
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
	c.Write(obj.([]byte))

	msgData, _ := bufio.NewReader(c).ReadString('\n')
	ackerr := handleAck(strings.TrimSpace(msgData), c)
	return ackerr
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
