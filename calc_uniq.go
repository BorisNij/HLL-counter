package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
	"os"
)

type HyperLogLog struct {
	registers     []int
	registerIndex uint
	b             uint
}

func (h HyperLogLog) Add(data []byte) HyperLogLog {
	x := createHash(data)
	k := 32 - h.b // first b bits
	r := leftmostActiveBit(x << h.b)
	j := x >> k

	if r > h.registers[j] {
		h.registers[j] = r
	}
	return h
}

func (h HyperLogLog) Count() uint64 {
	sum := 0.0
	m := float64(h.registerIndex)
	for _, v := range h.registers {
		sum += math.Pow(math.Pow(2, float64(v)), -1)
	}
	estimate := 0.79402 * m * m / sum
	// Handle small estimates
	if estimate <= 2.5*float64(h.registerIndex) {
		// Small range correction
		zeroCount := 0
		for _, b := range h.registers {
			if b == 0 {
				zeroCount++
			}
		}
		if zeroCount > 0 {
			estimate = float64(h.registerIndex) * math.Log(float64(h.registerIndex)/float64(zeroCount))
		}
	}
	return uint64(estimate)
}

// Merge merges another HyperLogLog into this one. The number of registers in each must be the same.
func (h HyperLogLog) Merge(h2 HyperLogLog) error {
	if h.registerIndex != h2.registerIndex {
		return fmt.Errorf("number of registers doesn't match: %d != %d", h.registerIndex, h2.registerIndex)
	}
	for j, r := range h2.registers {
		if r > h.registers[j] {
			h.registers[j] = r
		}
	}
	return nil
}

func leftmostActiveBit(x uint32) int {
	return 1 + bits.LeadingZeros32(x)
}

// create a 32-bit hash
func createHash(stream []byte) uint32 {
	h := fnv.New32()
	_, err := h.Write(stream)
	if err != nil {
		return 0
	}
	sum := h.Sum32()
	h.Reset()
	return sum
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <file_path>")
		return
	}

	// Open the file containing IP addresses
	filePath := os.Args[1]
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	//hll := NewHyperLogLog(1 << 16)
	var m uint = 1 << 16
	hll := HyperLogLog{
		registers:     make([]int, m),
		registerIndex: m,
		b:             uint(math.Ceil(math.Log2(float64(1 << 16)))),
	} // Initialize HyperLogLog with 65536 registers (2^16)

	// Read IP addresses from the file
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		ip := scanner.Text()
		hll = hll.Add([]byte(ip)) // Add each IP address to HyperLogLog
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return
	}

	// Estimate the number of unique addresses
	uniqueCount := hll.Count()
	fmt.Printf("Estimated number of unique IPv4 addresses: %d\n", uniqueCount)
}
