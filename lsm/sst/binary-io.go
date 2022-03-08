package sst

import (
	"encoding/binary"
	"unicode/utf8"
	"log"
	"os"
)

func binaryWrite(f *os.File, data SstEntry) {
  var bytes uint32 = uint32(utf8.RuneCountInString(data.Key))
  //f.Write()

  err := binary.Write(f, binary.LittleEndian, bytes)
  if err != nil {
    log.Fatal(err)
  }
  numBytes, _ := f.WriteString(data.Key)
  log.Printf("bytes = %d, numBytes = %d", bytes, numBytes)
}
