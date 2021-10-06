package objectstorage

import "fmt"

type Server struct {
	port    int
	running bool
}

func (s Server) Start(port int) {
	if s.running {
		fmt.Println("ERROR: Server already running")
		return
	}

	s.port = port
	s.running = true
	fmt.Println("Hello world! This is the Server")
}

func (s Server) Stop() {
	s.running = false
}
