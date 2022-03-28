package sst

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"unicode/utf8"
)

// readEntries reads all entries from the given SST file pointer and
// returns them as an array
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

// readEntry reads a single entry from the given SST file pointer
func readEntry(f *os.File) (SstEntry, error) {
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

	// read value length
	err = binary.Read(f, binary.LittleEndian, &length)
	if err != nil {
		log.Fatal(err)
		return e, err
	}
	//log.Println("value length", length)

	// read value
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

// writeSst creates an SST file and corresponding index file using the given data.
// seqNum is the sequence number of the latest entry.
// keysPerSegment is the number of keys that will be stored for each sparse index.
func writeSst(filename string, keys []string, m map[string]SstEntry, seqNum uint64, keysPerSegment int) {
	baseFilename := sstBaseFilename(filename)
	f, err := os.Create(baseFilename + ".bin")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	findex, err := os.Create(baseFilename + ".index")
	if err != nil {
		log.Fatal(err)
	}
	defer findex.Close()

	// write seq header to index file
	err = binary.Write(findex, binary.LittleEndian, seqNum)

	// write every nth entry to index file (sparse index)
	var offset int = 0
	for i, k := range keys {
		var e SstEntry
		e = m[k]
		bytes, err := writeEntry(f, &e)
		if err != nil {
			log.Fatal(err)
		}
		if (i % keysPerSegment) == 0 {
			writeKeyToIndex(findex, e.Key, offset)
		}
		offset += bytes
	}
}

func readIndexFile(filename string) ([]SstIndex, SstFileHeader, error) {
	indexFilename := indexFileForBin(filename)
	fp, err := os.Open(indexFilename)
	if err != nil {
		log.Fatal(err)
	}
	defer fp.Close()

	return readIndex(fp)
}

// readIndex reads and returns the contents of the given SST index file pointer.
func readIndex(f *os.File) ([]SstIndex, SstFileHeader, error) {
	var header SstFileHeader
	var index []SstIndex
	err := binary.Read(f, binary.LittleEndian, &header.Seq)
	if err != nil {
		log.Fatal(err)
		return index, header, err
	}

	var length int32
	for err == nil {
		var entry SstIndex
		// read key length
		err = binary.Read(f, binary.LittleEndian, &length)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		// read key
		var keybuf = make([]byte, int(length))
		_, err = io.ReadFull(f, keybuf)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Fatal(err)
			}
		}
		entry.Key = string(keybuf)

		//read offset
		err = binary.Read(f, binary.LittleEndian, &length)
		if err != nil {
			if err != io.EOF {
				log.Fatal(err)
			}
		}
		entry.offset = int(length)

		index = append(index, entry)
	}

	return index, header, nil
}

// writeKeyToIndex writes a sparse index to the given SST index file pointer.
// key is the SST key that is being written as a sparse index.
// offset is the byte offset of the key in the corresponding SST file.
func writeKeyToIndex(f *os.File, key string, offset int) error {
	// key length
	var bytes int32 = int32(utf8.RuneCountInString(key))
	err := binary.Write(f, binary.LittleEndian, bytes)
	if err != nil {
		log.Fatal(err)
		return err
	}
	// key
	_, err = f.WriteString(key)
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

// writeEntry writes data for a single key/value pair to file
func writeEntry(f *os.File, data *SstEntry) (int, error) {
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
