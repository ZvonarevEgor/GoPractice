package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	ErrEmptyArgs  MyError = "Not received the name of the file as argument!"
	ErrBrokenLink MyError = "An unexpected name. Make sure the file is in the extension '.txt'"
)

type FileName string
type MyError string
type MyMap func(string) map[string][][]string
type MyReduce func(map[string][][]string) map[string][]int

func (m MyError) Error() string {
	return string(m)
}

//Get the file name via command line arguments.
func GetLink(l *FileName) error {
	if len(os.Args) < 2 {
		return ErrEmptyArgs
	} else if arg := os.Args[1]; filepath.Ext(arg) != ".txt" {
		return ErrBrokenLink
	} else {
		*l = FileName(arg)
	}
	return nil
}

//Get the contents of the file.
func DataFromTxt(link FileName) (string, error)  {
	file, err := os.Open(string(link))
	if err != nil {
		return "", err
	}
	defer file.Close()
	content, err := ioutil.ReadFile(string(link))
	if err != nil {
		return "", err
	}
	return string(content), nil
}

func MapReduce(d string, M MyMap, R MyReduce) map[string][]int {
	result := R(M(d))
	return result
}

//Process incoming data by grouping values with the same keys.
func Map(d string) map[string][][]string {
	var arr = make(map[string][][]string)
	lines := strings.Split(d, ";")
	for _, line := range lines {
		elements := strings.Split(line, " ")
		elementsWithOutFirst := elements[1:]
		arr[elements[0]] = append(arr[elements[0]], elementsWithOutFirst)
	}
	return arr
}

//Сalculate all the values in the desired way.
func Reduce(arr map[string][][]string) map[string][]int {
	var result = make(map[string][]int)
	for key := range arr {
		resultSlice := make([]int, 6)
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

func main() {
	var link FileName
	if err := GetLink(&link); err != nil {
		fmt.Println(err)
	} else {
		data, err := DataFromTxt(link)
		if err == nil {
			result := MapReduce(data, Map, Reduce)
			PrintResult(result)
		} else {
			fmt.Println(err)
		}
	}
}
