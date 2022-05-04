package main

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"time"
)

func get(key string) {
	resp, err := http.Get("http://localhost:8080/kv/" + key)
	if err != nil {
		log.Fatalln(err)
	}
	defer resp.Body.Close()
	//Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	sb := string(body)
	log.Printf(sb)
}

func set(key string, contentType string, data []byte) {
	responseBody := bytes.NewBuffer(data)
	//Leverage Go's HTTP Post function to make request
	resp, err := http.Post("http://localhost:8080/kv/"+key, "application/json", responseBody)
	//Handle Error
	if err != nil {
		log.Fatalf("An Error Occured %v", err)
	}
	defer resp.Body.Close()
	//Read the response body
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}
	sb := string(body)
	log.Printf(sb)
}

func main() {
	//set("data-test", "text/plain", []byte("testing 1, 2, 3..."))
	//get("data-test")
	for i := 0; i < 100000; i++ {
		key := fmt.Sprintf("%d", i)
		doc := fmt.Sprintf("%d", time.Now().UnixNano())
		set(key, "text/plain", []byte(doc))
	}
}
