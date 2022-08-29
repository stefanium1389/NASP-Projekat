package SSTable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
)

// Write

func IndexSegToBin(key string, offset int) []byte {
	binKey := []byte(key)

	binOffset := make([]byte, 8)
	binary.LittleEndian.PutUint64(binOffset, uint64(offset))

	keySize := make([]byte, 8)
	binary.LittleEndian.PutUint64(keySize, uint64(len(binKey)))

	size := binary.LittleEndian.Uint64(keySize) + 16
	elem := make([]byte, 0, size)

	elem = append(elem, keySize...)
	elem = append(elem, binKey...)
	elem = append(elem, binOffset...)

	return elem
}

// Read

func ReadIndex(path string, key string, offset int64) int64 {
	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()

	_, err = file.Seek(offset, 0)
	Panic(err)
	reader := bufio.NewReader(file)

	keySize := make([]byte, 8)
	_, err = reader.Read(keySize)
	Panic(err)

	currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
	_, err = reader.Read(currentKey)
	Panic(err)

	a := string(currentKey)
	if key == a {
		dataOffset := make([]byte, 8)
		_, err = reader.Read(dataOffset)
		Panic(err)
		return int64(binary.LittleEndian.Uint64(dataOffset))
	} else {
		panic("Key not found")
	}
}

func CheckIndex(path string, key string, offset int64) (bool, int64) {
	ofs := ReadIndex(path, key, offset)
	if ofs != -1 {
		return true, ofs
	} else {
		return false, ofs
	}
}

//PrintIndex used for debugging, DELETE AFTER FINISHING

func PrintIndex(path string) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0700)
	Panic(err)
	defer file.Close()
	_, err = file.Seek(0, 0)
	Panic(err)
	br := bufio.NewReader(file)

	i := 1
	for err == nil {
		keySize := make([]byte, 8)
		_, err = br.Read(keySize)
		if err != nil {
			break
		}
		currentKey := make([]byte, binary.LittleEndian.Uint64(keySize))
		_, err = br.Read(currentKey)
		if err != nil {
			break
		}

		dataOffset := make([]byte, 8)
		_, err = br.Read(dataOffset)
		if err != nil {
			break
		}
		fmt.Println(i, ". Key size: ", binary.LittleEndian.Uint64(keySize),
			"; Key: ", string(currentKey),
			"; Offset in Data file: ", binary.LittleEndian.Uint64(dataOffset))
	}
	i++
}
