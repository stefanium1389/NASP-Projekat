package main

import (
	"bufio"
	"fmt"
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

func ReadInput() (string, string){
	fmt.Print("Unesite kljuc: ")
	reader := bufio.NewReader(os.Stdin)
	key, _ := reader.ReadString('\n')
	key = ReplaceWhiteSpace(key)
	fmt.Print("Unesite vrednost: ")
	value, _ := reader.ReadString('\n')
	value = ReplaceWhiteSpace(value)
	return key, value
}

func Put(){
	key, value := ReadInput()
	success := processor.Put(key, value)

	if success{
		fmt.Println("Dodavanje je uspesno.")
	}else{
		fmt.Println("Dodavanje nije uspelo.")
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
			//Get()
			break
		case "3":
			//Delete()
			break
		case "x":
			return
		default:
			fmt.Println("Pogresan unos.")
		}
	}
}
