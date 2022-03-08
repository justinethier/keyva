package sst

import (
	//"unicode/utf8"
	//"encoding/json"
	//"log"
	//"os"
)

func binaryWrite(fp *File, data SstEntry) {
  var bytes int = utf8.RuneCountInString(data.Key)
  //f.Write()
  numBytes := f.WriteString(data.Key)
}
