package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	// Open the file containing IP addresses.
	file, err := os.Open("ip_addr_5M_uniq_100M_total.txt")
	if err != nil {
		panic(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	// Set to store unique IP addresses.
	uniqueIPs := make(map[string]struct{})

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ip := scanner.Text()
		uniqueIPs[ip] = struct{}{}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// The number of unique IP addresses.
	fmt.Printf("The number of unique IP addresses is: %d\n", len(uniqueIPs))
}
