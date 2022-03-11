package sst

import (
	"encoding/binary"
	"unicode/utf8"
	"io"
	"log"
	"os"
)

func readEntries(f *os.File) []SstEntry {
  var lis []SstEntry
  var err error
  var e SstEntry

  for err == nil {
    e, err = readEntry(f)
    if err == nil {
      lis = append(lis, e)
    }
  }
  return lis
}

func readEntry(f *os.File) (SstEntry, error){
  var length int32
  var e SstEntry

  err := binary.Read(f, binary.LittleEndian, &length)
  if err != nil {
    if err != io.EOF {
      log.Fatal(err)
    }
    return e, err
  }
  //log.Println("key length", length)

  var keybuf = make([]byte, int(length))
  _, err = io.ReadFull(f, keybuf)
  e.Key = string(keybuf)

  // TODO: read value length
  err = binary.Read(f, binary.LittleEndian, &length)
  if err != nil {
    log.Fatal(err)
    return e, err
  }
  //log.Println("value length", length)

  // TODO: use f.ReadFull to read value
  var valbuf = make([]byte, int(length))
  _, err = io.ReadFull(f, valbuf)
  e.Value = valbuf

  err = binary.Read(f, binary.LittleEndian, &e.Deleted)
  if err != nil && err != io.EOF {
    log.Fatal(err)
    return e, err
  }
  //log.Println("entry", e)
  return e, nil
}

func writeSst(filename string, keys []string, m map[string]SstEntry, seqNum uint64) {
  f, err := os.Create(filename + ".bin")
  if err != nil {
    log.Fatal(err)
  }
  defer f.Close()

  findex, err := os.Create(filename + ".index")
  if err != nil {
    log.Fatal(err)
  }
  defer findex.Close()

  // write seq header to index file
  err = binary.Write(findex, binary.LittleEndian, seqNum)

  // TODO: write every nth entry tp index file (sparse index)
  var offset int = 0
	for i, k := range keys {
		var e SstEntry
		e = m[k]
		bytes, err := writeEntry(f, e)
		if err != nil {
      log.Fatal(err)
    }
    if (i % 2) == 0 { // TODO: larger and configurable interval
      writeIndex(findex, e, bytes)
    }
    offset += bytes
	}
}

func readIndex(f *os.File) ([]SstIndex, SstFileHeader, error) {
  var header SstFileHeader
  var index []SstIndex
  err := binary.Read(f, binary.LittleEndian, &header.Seq)
  if err != nil {
    log.Fatal(err)
    return index, header, err
  }

  for contents of index
    read key length
    read key
    read offset
    package into SstIndex, append to list

  return index, header, nil
}

func writeIndex(f *os.File, data SstEntry, offset int) error {
  // key length
  var bytes int32 = int32(utf8.RuneCountInString(data.Key))
  err := binary.Write(f, binary.LittleEndian, bytes)
  if err != nil {
    log.Fatal(err)
    return err
  }
  // key
  _, err = f.WriteString(data.Key)
  if err != nil {
    log.Fatal(err)
    return err
  }
  // offset
  err = binary.Write(f, binary.LittleEndian, int32(offset))
  if err != nil {
    log.Fatal(err)
    return err
  }
  return nil
}

func writeEntry(f *os.File, data SstEntry) (int, error){
  var bcount int = 0
  var bytes int32 = int32(utf8.RuneCountInString(data.Key))

  err := binary.Write(f, binary.LittleEndian, bytes)
  if err != nil {
    log.Fatal(err)
    return bcount, err
  }
  bcount += 4

  numBytes, err := f.WriteString(data.Key)
  if err != nil {
    log.Fatal(err)
    return bcount, err
  }
  bcount += numBytes
  //log.Printf("bytes = %d, numBytes = %d", bytes, numBytes)

  bytes = int32(len(data.Value))
  err = binary.Write(f, binary.LittleEndian, bytes)
  if err != nil {
    log.Fatal(err)
    return bcount, err
  }
  bcount += 4

  numBytes, err = f.Write(data.Value)
  if err != nil {
    log.Fatal(err)
    return bcount, err
  }
  bcount += numBytes
  //log.Printf("bytes = %d, numBytes = %d", bytes, numBytes)

  err = binary.Write(f, binary.LittleEndian, data.Deleted)
  if err != nil {
    log.Fatal(err)
    return bcount, err
  }
  bcount += 1

  return bcount, nil
}
