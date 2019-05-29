package main

import "fmt"

func main() {
	var a []string
	a = append(a, "first")
	once := true
	var el string
	for len(a) > 0 {
		el, a = a[0], a[1:]
		if once {
			a = append(a, "second")
			once = false
		}
		fmt.Println(el)
	}
}
