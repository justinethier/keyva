package sst

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"github.com/justinethier/keyva/util"
	"log"
	"os"
	"regexp"
	"strconv"
)

// Create creates a new SST file from given data
func Create(filename string, keys []string, m map[string]SstEntry, seqNum uint64) {
	f, err := os.Create(filename)
	check(err)

	defer f.Close()

	writeSstFileHeader(f, seqNum)

	for _, k := range keys {
		var e SstEntry
		e = m[k]
		writeSstEntry(f, &e, false)
	}
}

// Open opens the given file and returns the SST file header along with a buffered reader.
func Open(filename string) (*os.File, *bufio.Reader, SstFileHeader, error) {
	var header SstFileHeader
	var reader *bufio.Reader
	f, err := os.Open(filename)
	if err != nil {
		return f, reader, header, err
	}

	reader = bufio.NewReader(f)
	str, e := util.Readln(reader)
	check(e)
	err = json.Unmarshal([]byte(str), &header)
	check(e)
	//fmt.Println("DEBUG SST header", header)
	return f, reader, header, nil
}

// Readln reads the next entry from an SST file using the given buffered reader.
func Readln(reader *bufio.Reader) (SstEntry, error) {
	var entry SstEntry
	str, err := util.Readln(reader)
	if err != nil {
		return entry, err
	}
	err = json.Unmarshal([]byte(str), &entry)
	check(err)
	return entry, err
}

//
func Load(filename string) ([]SstEntry, SstFileHeader) {
	var buf []SstEntry
	var header SstFileHeader

	f, err := os.Open(filename)
	if err != nil {
		log.Println("Load error", err)
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

func writeSstFileHeader(f *os.File, seqNum uint64) {
	header := SstFileHeader{seqNum}
	b, err := json.Marshal(header)
	check(err)
	_, err = f.Write(b)
	check(err)
	_, err = f.Write([]byte("\n"))
	check(err)
}

func writeSstEntry(f *os.File, e *SstEntry, removeDeleted bool) {
	if (e.Deleted && removeDeleted) || e == nil {
		return
	}

	b, err := json.Marshal(&e)
	check(err)

	_, err = f.Write(b)
	check(err)

	_, err = f.Write([]byte("\n"))
	check(err)
}


// Levels returns the names of any directories containing consolidated
// SST files at levels greater than level 0. This implies the data is
// organized in non-overlapping regions across files at that level.
func Levels(path string) []string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}

	var lvls []string
	for _, file := range files {
		matched, _ := regexp.Match(`^level-[0-9]*`, []byte(file.Name()))
		if matched && file.IsDir() {
			lvls = append(lvls, file.Name())
		}
	}

	return lvls
}

func PathForLevel(base string, level int) string {
	if level == 0 {
		return base
	}

	return fmt.Sprintf("%s/level-%d", base, level)
}

// Filenames returns names of the SST files under path
func Filenames(path string) []string {
	var sstFiles []string
	files, err := ioutil.ReadDir(path)
	if err == nil {
		for _, file := range files {
			matched, _ := regexp.Match(`^sorted-string-table-[0-9]*\.json`, []byte(file.Name()))
			if matched && !file.IsDir() {
				sstFiles = append(sstFiles, file.Name())
			}
		}
	}

	return sstFiles
}

// NextFilename returns the name of the next SST file in given directory
func NextFilename(path string) string {
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
