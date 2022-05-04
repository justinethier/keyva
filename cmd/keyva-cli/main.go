package main

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	//"github.com/knetic/govaluate"
	"github.com/justinethier/keyva/lsm"
)

var db = lsm.New("data", 5)

func printRepl() {
	fmt.Print("keyva> ")
}

func recoverExp(text string) {
	if r := recover(); r != nil {
		fmt.Println("recovered", r)
	}
}

func printInvalidCmd(text string, reader *bufio.Reader) {
	// We might have a panic here we so need DEFER + RECOVER
	defer recoverExp(text)
	// \n Will be ignored
	t := strings.TrimSuffix(text, "\n")
	if len(t) > 4 && t[:4] == "get " {
		key := t[4:]
		dbGet(key)
	} else if len(t) > 4 && t[:4] == "set " {
		key := t[4:]
		dbSet(key, reader)
	} else if len(t) > 4 && t[:4] == "del " {
		key := t[4:]
		dbDelete(key)
	} else if len(t) > 6 && t[:6] == "merge " {
		level := t[6:]
		dbMerge(level)
	} else if t != "" {
		fmt.Println("Unknown command", t)
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
	fmt.Println("Welcome to keyva REPL! ")
	fmt.Println("Written by Justin Ethier - 2022 ")
	fmt.Println("Available commands: ")
	fmt.Println("get    - Display value for the given key")
	fmt.Println("set    - Set value of the given key")
	fmt.Println("del    - Delete value of the given key")
	fmt.Println("merge  - Merge data in level l of the DB")
	fmt.Println("help   - Display usage information")
	fmt.Println("cls    - Clear the terminal screen ")
	fmt.Println("time   - Prints current date / time ")
	fmt.Println("exit   - Exits the REPL ")
	fmt.Println("")
}

func cls() {
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func now() {
	fmt.Println("keyva> ", time.Now().Format(time.RFC850))
}

func dbGet(key string) {
	val, found := db.Get(key)
	if found {
		fmt.Println(string(val))
	} else {
		fmt.Println("Key not found")
	}
}

func dbSet(key string, reader *bufio.Reader) {
	fmt.Println("Value to set: ")
	value := get(reader)
	db.Set(key, []byte(value))
}

func dbDelete(key string) {
	db.Delete(key)
}

func dbMerge(level string) {
	l, err := strconv.Atoi(level)
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	db.Merge(l)
}

func main() {
	db.SetMergeSettings(lsm.MergeSettings{MaxLevels: 5, NumberOfSstFiles: 2, Interval: 120 * time.Second})
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
			printInvalidCmd(text, reader)
		}
		printRepl()
	}
	fmt.Println("Bye!")

}
