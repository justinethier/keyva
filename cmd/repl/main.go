package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"time"

	//"github.com/knetic/govaluate"
	"github.com/justinethier/keyva/lsm"
)

var db = lsm.New("data", 5)

func printRepl() {
	fmt.Print("go-repl> ")
}

func recoverExp(text string) {
	if r := recover(); r != nil {
		fmt.Println("go-repl> unknow command ", text)
	}
}

func printInvalidCmd(text string) {
	// We might have a panic here we so need DEFER + RECOVER
	defer recoverExp(text)
	// \n Will be ignored
	t := strings.TrimSuffix(text, "\n")
	if t[:4] == "get " {
		key := t[4:]
		dbGet(key)
	} else if t[:4] == "set " {
		key := t[4:]
		value := "TODO"
		dbSet(key, value)
	} else if t[:4] == "del " {
		key := t[4:]
		dbDelete(key)
	} else if t != "" {
		fmt.Println("go-repl> unknown command " + t)
		//expression, errExp := govaluate.NewEvaluableExpression(text)
		//result, errEval := expression.Evaluate(nil)
		//// Before we need to know if is not a Math expr
		//if errExp == nil && errEval == nil {
		//	fmt.Println("go-repl>", result)
		//} else {
		//	fmt.Println("go-repl> unknow command " + t)
		//}
	}
}

func get(r *bufio.Reader) string {
	t, _ := r.ReadString('\n')
	return strings.TrimSpace(t)
}

func shouldContinue(text string) bool {
	if strings.EqualFold("exit", text) {
		return false
	}
	return true
}

func help() {
	fmt.Println("go-repl> Welcome to Go Repl! ")
	fmt.Println("go-repl> Wrote by Diego Pacheco - 2018 ")
	fmt.Println("go-repl> This Are the Avaliable commands: ")
	fmt.Println("go-repl> help   - Show you the Help")
	fmt.Println("go-repl> cls    - Clear the Terminal Screen ")
	fmt.Println("go-repl> exit   - Exits the Go REPL ")
	fmt.Println("go-repl> 1 + 2  - Its possible todo Math expressions: true == true, 4 * 6 / 2, 2 > 1 ")
	fmt.Println("go-repl> time   - Prints current date / time ")
	fmt.Println("go-repl> ")
}

func cls() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func now() {
	fmt.Println("go-repl> ", time.Now().Format(time.RFC850))
}

func dbGet(key string) {
	val, found := db.Get(key)
	if found {
		fmt.Println(string(val))
	} else {
		fmt.Println("Key not found")
	}
}

func dbSet(key string, value string) {
	db.Set(key, []byte(value))
}

func dbDelete(key string) {
	db.Delete(key)
}

func main() {
	commands := map[string]interface{}{
		"help": help,
		"cls":  cls,
		"time": now,
	}
	reader := bufio.NewReader(os.Stdin)
	help()
	printRepl()
	text := get(reader)
	for ; shouldContinue(text); text = get(reader) {
		if value, exists := commands[text]; exists {
			value.(func())()
		} else {
			printInvalidCmd(text)
		}
		printRepl()
	}
	fmt.Println("Bye!")

}
