package sst

import (
	"encoding/binary"
	"unicode/utf8"
	"log"
	"os"
)

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
