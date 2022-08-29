package main

import (
	"bufio"
	"fmt"
	"main/CountMinSketch"
	"main/HyperLogLog"
	"main/Processor"
	"os"
	"strings"
)

var processor *Processor.Processor

func ReplaceWhiteSpace(str string) string{
	str = strings.Replace(str, "\n", "", 1)
	str = strings.Replace(str, "\r", "", 1)
	return str
}

func ReadInput(put bool) (string, string){
	fmt.Print("Unesite kljuc: ")
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = ReplaceWhiteSpace(key)
	if !put{
		return key, ""
	}
	fmt.Print("Unesite vrednost: ")
	value, _ := reader.ReadString('\n')
	value = ReplaceWhiteSpace(value)
	return key, value
}

func Put(){
	key, value := ReadInput(true)
	success := processor.Put(key, value)

	if success{
		fmt.Println("Dodavanje je uspesno.")
	}else{
		fmt.Println("Dodavanje nije uspelo.")
	}
}

func Get(){
	key, _ := ReadInput(false)
	value, found := processor.Get(key)
	if !found{
		fmt.Println("Neuspesna pretraga. ")
	}else{
		fmt.Println(key + ": " + value)
	}
}

func Delete(){
	key, _ := ReadInput(false)
	deleted := processor.Delete(key)
	if !deleted{
		fmt.Println("Neuspesno brisanje. ")
	}else{
		fmt.Println("Uspesno obrisano. ")
	}
}

func HLL(){
	fmt.Println("1. Izaberi postojeci HLL")
	fmt.Println("2. Dodaj HLL")
	fmt.Println("x. Odustani")
	reader := bufio.NewReader(os.Stdin)
	choice, err := reader.ReadString('\n')
	choice = ReplaceWhiteSpace(choice)
	if err != nil {
		panic(err.Error())
	}
	if choice == "1"{
		key, _ := ReadInput(false)
		key += "_hll"
		data, found := processor.Get(key)
		if !found{
			fmt.Println("Ne postoji HLL sa datim kljucem. ")
			return
		}
		hll := HyperLogLog.HyperLogLog{}
		hll.Decode([]byte(data))
	}else if choice == "2"{
		key, _ := ReadInput(false)
		key += "_hll"
		_, found := processor.Get(key)
		if found{
			fmt.Println("Vec postoji HLL sa ovim kljucem. ")
			return
		}
		hll := HyperLogLog.NewHyperLogLog(6)
		data := hll.Encode()
		processor.Put(key, data)
		fmt.Println("Uspesno je dodat novi HLL")

	}else if choice != "x"{
		fmt.Println("Nepostojeca opcija. ")
	}
}

func CMS(){
	fmt.Println("1. Izaberi postojeci CMS")
	fmt.Println("2. Dodaj novi CMS")
	fmt.Println("x. Odustani")
	reader := bufio.NewReader(os.Stdin)
	choice, err := reader.ReadString('\n')
	choice = ReplaceWhiteSpace(choice)
	if err != nil {
		panic(err.Error())
	}
	if choice == "1"{
		key, _ := ReadInput(false)
		key += "_cms"
		data, found := processor.Get(key)
		if !found{
			fmt.Println("Ne postoji CMS sa datim kljucem. ")
			return
		}
		cms := CountMinSketch.CountMinSketch{}
		cms.Decode([]byte(data))
	}else if choice == "2"{
		key, _ := ReadInput(false)
		key += "_cms"
		_, found := processor.Get(key)
		if found{
			fmt.Println("Vec postoji CMS sa ovim kljucem. ")
			return
		}
		cms := CountMinSketch.NewCountMinSketch(0.1, 0.1)
		data := cms.Encode()
		processor.Put(key, data)
		fmt.Println("Uspesno je dodat novi CMS")

	}else if choice != "x"{
		fmt.Println("Nepostojeca opcija. ")
	}
}

func main(){
	processor = Processor.NewProcessor()
	Menu()

}

func Menu(){
	for true{
		fmt.Println("\n\nIzaberite zeljenu operaciju: ")
		fmt.Println("1. Put")
		fmt.Println("2. Get")
		fmt.Println("3. Delete")
		fmt.Println("4. Compaction")
		fmt.Println("5. HLL")
		fmt.Println("6. CMS")
		fmt.Println("x. Izlaz\n")

		reader := bufio.NewReader(os.Stdin)
		choice, err := reader.ReadString('\n')
		choice = ReplaceWhiteSpace(choice)
		if err != nil {
			panic(err.Error())
		}

		switch choice {
		case "1":
			Put()
			break
		case "2":
			Get()
			break
		case "3":
			Delete()
			break
		case "4":
			break
		case "5":
			HLL()
			break
		case "6":
			CMS()
			break
		case "x":
			return
		default:
			fmt.Println("Pogresan unos.")
		}
	}
}
