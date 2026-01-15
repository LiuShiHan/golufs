package main

import (
	"fmt"
	pb "github.com/LiuShiHan/golufs/pb/proto"
)

func main() {
	fmt.Print("Hello World")
	a := pb.FileInfoRequest{}
	fmt.Println(a)
}
