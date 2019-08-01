package main

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
)

type FileName string
type ErrEmptyArgs int
type ErrBrokenLink string
type MyMap func(string) map[string][][]string
type MyReduce func(map[string][][]string) map[string][]int

func (e ErrEmptyArgs) Error() string {
	return fmt.Sprintf("Not received the name of the file as argument!")
}

func (e ErrBrokenLink) Error() string {
	return fmt.Sprintf("An unexpected name. Make sure the file is in the extension '.txt'")
}

//Get the file name via command line arguments.
func GetLink(l *FileName) error {
	if len(os.Args) < 2 {
		return ErrEmptyArgs(0)
	} else if os.Args[1][len(os.Args[1])-3:] != "txt" {
		return ErrBrokenLink("notTxt")
	} else {
		*l = FileName(os.Args[1])
	}
	return nil
}

//Get the contents of the file.
func DataFromTxt(l FileName) string {
	file, err := os.Open(string(l))
	defer file.Close()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	stat, err := file.Stat()
	if err != nil {
		fmt.Println(err)
		return ""
	}
	data := make([]byte, stat.Size())
	for {
		_, err := file.Read(data)
		if err == io.EOF {
			break //Stop reading the file if it is finished.
		}
	}
	return string(data)
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
		fmt.Printf("%v: %v %v %v %v %v %v\n", key, m[key][0], m[key][1], m[key][2], m[key][3], m[key][4], m[key][5])
	}
}

func main() {
	var link FileName
	if err := GetLink(&link); err != nil {
		fmt.Println(err)
	} else {
		data := DataFromTxt(link)
		result := MapReduce(data, Map, Reduce)
		PrintResult(result)
	}
}
