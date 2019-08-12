package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type MyError string

const (
	ErrEmptyArgs    MyError = "Not received the name of the file as argument and number of hours!"
	ErrBrokenLink   MyError = "An unexpected name. Make sure the file is in the extension '.txt' and correct number of hours"
	ErrBrokenNumber MyError = "You did not pass a number as the second argument."
)

func (m MyError) Error() string {
	return string(m)
}

//Get the file name via command line arguments.
func GetArgs(fileFields *Data) error {
	args := os.Args
	if len(args) < 3 {
		return ErrEmptyArgs
	}
	fileName := args[1]
	if filepath.Ext(fileName) != ".txt" {
		return ErrBrokenLink
	}
	hours, err := strconv.Atoi(args[2])
	if err != nil {
		return ErrBrokenNumber
	}
	fileFields.FileName = fileName
	fileFields.Hours = hours
	return nil
}

//Get the contents of the file.
func DataFromTxt(fileFields *Data) error {
	file, err := os.Open(fileFields.FileName)
	if err != nil {
		return err
	}
	defer file.Close()
	content, err := ioutil.ReadFile(fileFields.FileName)
	fileFields.Content = string(content)
	if err != nil {
		return err
	}
	return nil
}

func MapReduce(f *Data) map[string][]int {
	arr := f.Map()
	result := f.Reduce(arr)
	return result
}

//Process incoming data by grouping values with the same keys.
func (fileFields *Data) Map() map[string][][]string {
	var arr = make(map[string][][]string)
	lines := strings.Split(fileFields.Content, ";")
	for _, line := range lines {
		elements := strings.Split(line, " ")
		elementsWithOutFirst := elements[1 : fileFields.Hours+1]
		arr[elements[0]] = append(arr[elements[0]], elementsWithOutFirst)
	}
	return arr
}

//Сalculate all the values in the desired way.
func (fileFields *Data) Reduce(arr map[string][][]string) map[string][]int {
	var result = make(map[string][]int)
	for key := range arr {
		resultSlice := make([]int, fileFields.Hours)
		for _, slice := range arr[key] {
			for i, elementStr := range slice {
				element, _ := strconv.Atoi(elementStr)
				resultSlice[i] = resultSlice[i] + element
			}
		}
		result[key] = resultSlice
	}
	return result
}

//Format the string and output the contents.
func PrintResult(m map[string][]int) {
	for key := range m {
		fmt.Print("\n", key, ": ")
		for _, item := range m[key] {
			fmt.Print(item, " ")
		}
	}
}

type Data struct {
	FileName string
	Hours    int
	Content  string
}

func main() {
	var fileFields Data
	if err := GetArgs(&fileFields); err != nil {
		log.Fatalln(err)
	}
	if err := DataFromTxt(&fileFields); err != nil {
		log.Fatalln(err)
	}
	result := MapReduce(&fileFields)
	PrintResult(result)
}
