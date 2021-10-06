package objectstorage

import "fmt"

type Server struct {
	port int
}

func (s Server) start() {
	fmt.Println("Hello world! This is the Server")
}
