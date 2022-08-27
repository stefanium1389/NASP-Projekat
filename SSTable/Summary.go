package SSTable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Summary struct {
	FirstKey, LastKey string
	Elements          map[string]int
}

// First/Last Key Size = 8B & Offset = 8B

func WriteSummary(summary *Summary, file *os.File) {
	firstEl := []byte(summary.FirstKey)
	firstElSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(firstElSize, uint64(len(firstEl)))
	first := make([]byte, 0, binary.LittleEndian.Uint64(firstElSize)+8)
	first = append(first, firstElSize...)
	first = append(first, firstEl...)
	_, err := file.Write(first)
	Panic(err)

	lastEl := []byte(summary.LastKey)
	lastElSize := make([]byte, 8)
	binary.LittleEndian.PutUint64(lastElSize, uint64(len(lastEl)))
	last := make([]byte, 0, binary.LittleEndian.Uint64(lastElSize)+8)
	last = append(last, lastElSize...)
	last = append(last, lastEl...)
	_, err = file.Write(last)
	Panic(err)

	for key, offset := range summary.Elements {
		binaryInfo := IndexSegToBin(key, offset)
		_, err = file.Write(binaryInfo)
		Panic(err)
	}
}

func ReadSummary(path string, key string) int64 {
	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(0, 0)
	Panic(err)
	br := bufio.NewReader(file)

	keySize := make([]byte, 8)
	_, err = br.Read(keySize)
	Panic(err)
	firstElement := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = br.Read(firstElement)
	Panic(err)
	if key < string(firstElement) {
		return -1
	}
	keySize = make([]byte, 8)
	_, err = br.Read(keySize)
	Panic(err)
	lastElement := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = br.Read(lastElement)
	Panic(err)
	if key > string(lastElement) {
		return -1
	}

	for err == nil {
		keySize = make([]byte, 8)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(currentKey)
		if err != nil {
			break
		}
		offset := make([]byte, 8)
		_, err = br.Read(offset)
		if err != nil {
			break
		}
		if key == string(currentKey) {

			return int64(binary.LittleEndian.Uint64(offset))
		}
	}
	if err == io.EOF {
		return -1
	} else {
		panic(err)
	}
}

func PrintSummary(path string) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(0, 0)
	Panic(err)
	br := bufio.NewReader(file)

	keySize := make([]byte, 8)
	_, err = br.Read(keySize)
	Panic(err)
	firstElement := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = br.Read(firstElement)
	Panic(err)
	fmt.Println("First element of Index: ", string(firstElement))

	keySize2 := make([]byte, 8)
	_, err = br.Read(keySize2)
	Panic(err)
	lastElement := make([]byte, binary.LittleEndian.Uint64(keySize2))
	_, err = br.Read(lastElement)
	Panic(err)
	fmt.Println("\nLast element of Index: ", string(lastElement))

	i := 1
	for err == nil {
		keySize = make([]byte, 8)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(currentKey)
		if err != nil {
			break
		}
		offset := make([]byte, 8)
		_, err = br.Read(offset)
		if err != nil {
			break
		}
		fmt.Println(string(i), ". Key: ", string(currentKey), " Offset: ", binary.LittleEndian.Uint64(offset))
	}
}
