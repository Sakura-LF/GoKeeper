package main

import (
	"GoKeeper"
	"fmt"
)

func main() {
	opts := GoKeeper.DefaultOptions
	db, err := GoKeeper.Open(opts)
	if err != nil {
		panic(err)
	}
	// put data
	//err = db.Put([]byte("name"), []byte("Sakura"))
	//if err != nil {
	//	panic(err)
	//	return
	//}

	// get data
	value, err := db.Get([]byte("name"))
	if err != nil {
		panic(err)
		return
	}
	fmt.Println("value:", string(value))

	// delete data
	//err = db.Delete([]byte("name"))
	//if err != nil {
	//	panic(err)
	//}
}
