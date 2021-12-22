package lsm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/justinethier/keyva/util"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
)

func createSstFile(filename string, keys []string, m map[string]SstEntry, seqNum uint64) {
	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	header := SstFileHeader{seqNum}
	b, err := json.Marshal(header)
	check(err)
	_, err = f.Write(b)
	check(err)
	_, err = f.Write([]byte("\n"))
	check(err)

	for _, k := range keys {
		b, err := json.Marshal(m[k])
		check(err)

		_, err = f.Write(b)
		check(err)

		_, err = f.Write([]byte("\n"))
		check(err)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func nextSstFilename(path string) string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	var sstFiles []string
	for _, file := range files {
		matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
		if matched && !file.IsDir() {
			//fmt.Println(file.Name(), file.IsDir())
			sstFiles = append(sstFiles, file.Name())
		}
	}

	if len(sstFiles) > 0 {
		var latest = sstFiles[len(sstFiles)-1][20:24]
		n, _ := strconv.Atoi(latest)
		return fmt.Sprintf("sorted-string-table-%04d.json", n+1)
	}

	return "sorted-string-table-0000.json"
}

func loadEntriesFromSstFile(filename string, path string) ([]SstEntry, SstFileHeader) {
	var buf []SstEntry
	var header SstFileHeader

	f, err := os.Open(path + "/" + filename)
	if err != nil {
		return buf, header
	}

	defer f.Close()

	r := bufio.NewReader(f)
	str, e := util.Readln(r)
	check(e)
	err = json.Unmarshal([]byte(str), &header)
	check(e)
	//fmt.Println("DEBUG SST header", header)

	str, e = util.Readln(r)
	for e == nil {
		var data SstEntry
		err = json.Unmarshal([]byte(str), &data)
		check(err)
		//fmt.Println(data)
		buf = append(buf, data)
		str, e = util.Readln(r)
	}

	return buf, header
}

