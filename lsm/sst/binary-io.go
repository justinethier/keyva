package sst

import (
	"encoding/binary"
	"unicode/utf8"
	"io"
	"log"
	"os"
)

func readEntries(f *os.File) ([]SstEntry, error) {
  for {
    _, err := readEntry(f)
    if err != nil {
      break
    }
  }
  return nil, nil
}

func readEntry(f *os.File) (SstEntry, error){
  var length int32
  var e SstEntry

  err := binary.Read(f, binary.LittleEndian, &length)
  if err != nil {
    log.Fatal(err)
    return e, err
  }
  log.Println("key length", length)

  var keybuf = make([]byte, int(length))
  _, err = io.ReadFull(f, keybuf)
  e.Key = string(keybuf)

  // TODO: read value length
  err = binary.Read(f, binary.LittleEndian, &length)
  if err != nil {
    log.Fatal(err)
    return e, err
  }
  log.Println("value length", length)

  // TODO: use f.ReadFull to read value
  var valbuf = make([]byte, int(length))
  _, err = io.ReadFull(f, valbuf)
  e.Value = valbuf

  err = binary.Read(f, binary.LittleEndian, &e.Deleted)
  if err != nil {
    log.Fatal(err)
    return e, err
  }
  log.Println("entry", e)

  return e, nil
}

func writeEntries(f *os.File, entries []SstEntry) (int, error){
  var offset int = 0
  for _, e := range entries {
    bytes, err := writeEntry(f, e)
    if err != nil {
      return offset, err
    }
    offset += bytes
    // TODO: keep track of every nth key/offset for the sparse index
  }

  return offset, nil
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
