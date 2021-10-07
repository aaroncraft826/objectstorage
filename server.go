package objectstorage

import (
	"bufio"
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
		s.handleMessage(strings.TrimSpace(msgData), c)
	}

	c.Close()
}

func (s *Server) handleMessage(msg string, c net.Conn) {
	msgValues := strings.Split(msg, "|")
	msgType := msgValues[0]

	switch msgType {
	case PUT.String():
		key := msgValues[1]
		byteSize, err := strconv.Atoi(msgValues[2])
		s.put(key, byteSize, c)
	case GET.String():
		key := msgValues[1]
		s.get(key, c)
	case DELETE.String():
		key := msgValues[1]
		s.delete(key, c)
	case LIST.String():
		s.list(c)
	case ACKNOWLEDGE.String():
		writeAck(FAILURE, c)
	default:
		writeAck(FAILURE, c)
	}
}

func (s *Server) put(key string, byteSize int, c net.Conn) {
	var obj = make([]byte, byteSize)

	val, err := c.Read(obj)
	if err != nil {
		fmt.Println(err)
		writeAck(FAILURE, c)
		return
	}

	s.dataStorage.Store(key, obj)
	writeAck(SUCCESS, c)
}

func (s *Server) get(key string, c net.Conn) {
	obj, found := s.dataStorage.Load(key)
	if !found {
		writeAck(EXISTERROR, c)
		return
	}
	writeAck(SUCCESS, c)

	byteSize := len(obj.([]byte))
	writeMsg(GET.String()+"|"+strconv.Itoa(byteSize), c)
	c.Write(obj.([]byte))
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
