package main

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io"
	"math"
	"math/bits"
	"os"
	"runtime"
)

var (
	exp32 = math.Pow(2, 32)
)

type HyperLogLog struct {
	registers            []int
	totalRegisters       uint    // m - number of registers from Flajolet et al. 2007
	m                    float64 // float value of total registers (same as m above but decimal)
	bitsForRegisterIndex uint    // precision (typically between 4 and 16)
	biasCorrectionConst  float64 // alpha_m from Flajolet et al. 2007
	valueBitCount        uint    // number of bits after chopping off the number of bitsForRegisterIndex aka register width
}

func NewHyperLogLog(bitsForRegisterIndex uint) HyperLogLog {
	totalRegisters := uint(1 << bitsForRegisterIndex) // Calculate total registers as 2^bitsForRegisterIndex
	var biasCorrectionConstant float64
	switch totalRegisters {
	case 16:
		biasCorrectionConstant = 0.673
	case 32:
		biasCorrectionConstant = 0.697
	case 64:
		biasCorrectionConstant = 0.709
	default:
		biasCorrectionConstant = 0.7213 / (1.0 + 1.079/float64(totalRegisters))
	}
	alpha := biasCorrectionConstant

	return HyperLogLog{
		registers:            make([]int, totalRegisters),
		totalRegisters:       totalRegisters,
		m:                    float64(totalRegisters),
		bitsForRegisterIndex: bitsForRegisterIndex,
		biasCorrectionConst:  alpha,
		valueBitCount:        32 - bitsForRegisterIndex,
	}
}

func (thisHll *HyperLogLog) Add(data []byte) {
	hashedData := createHash(data)
	leadingZeros := bits.LeadingZeros32(hashedData << thisHll.bitsForRegisterIndex)
	firstSetBitIndex := 1 + leadingZeros
	registerIndex := hashedData >> thisHll.valueBitCount

	if firstSetBitIndex > thisHll.registers[registerIndex] {
		thisHll.registers[registerIndex] = firstSetBitIndex
	}
}

func (thisHll *HyperLogLog) Count() uint64 {
	sum := 0.0
	for _, registerValue := range thisHll.registers {
		sum += math.Pow(math.Pow(2, float64(registerValue)), -1)
	}
	Z := 1.0 / sum // the harmonic mean of the register values aka "indicator" function
	estimate := thisHll.biasCorrectionConst * thisHll.m * thisHll.m * Z
	if estimate <= 5.0/2.0*float64(thisHll.totalRegisters) {
		// Small range correction
		zeroCount := 0
		for _, registerValue := range thisHll.registers {
			if registerValue == 0 {
				zeroCount++
			}
		}
		if zeroCount > 0 {
			estimate = float64(thisHll.totalRegisters) * math.Log(float64(thisHll.totalRegisters)/float64(zeroCount))
		}
	} else if estimate > 1.0/30.0*exp32 {
		// Large range correction
		estimate = -exp32 * math.Log(1-estimate/exp32)
	}
	return uint64(estimate)
}

func (thisHll *HyperLogLog) Merge(anotherHll *HyperLogLog) error {
	if thisHll.totalRegisters != anotherHll.totalRegisters {
		return fmt.Errorf("number of registers doesn't match: %d != %d", thisHll.totalRegisters, anotherHll.totalRegisters)
	}
	for i, registerValue := range anotherHll.registers {
		if registerValue > thisHll.registers[i] {
			thisHll.registers[i] = registerValue
		}
	}
	return nil
}

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
func processFileConcurrently(filePath string, precision uint) (*HyperLogLog, error) {
	// Get the number of CPU cores
	numCores := runtime.NumCPU()

	// Open the file
	partFile, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %v", err)
	}
	defer partFile.Close()

	// Get the file size
	fileInfo, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("error getting file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Calculate the size of each part
	partSize := fileSize / int64(numCores)

	// Create a channel to collect partial HyperLogLog results
	results := make(chan *HyperLogLog, numCores)
	errors := make(chan error, numCores)

	// Spawn worker goroutines
	for i := 0; i < numCores; i++ {
		startOffset := int64(i) * partSize
		var endOffset int64
		if i == numCores-1 {
			endOffset = fileSize // Last part takes the remainder
		} else {
			endOffset = startOffset + partSize
		}

		go func(start, end int64) {
			hll := NewHyperLogLog(precision)

			_, err = partFile.Seek(start, 0)
			if err != nil {
				errors <- fmt.Errorf("error seeking file: %v", err)
				return
			}

			// Process the part line by line
			reader := bufio.NewReader(partFile)
			currentOffset := start
			for currentOffset < end {
				line, err := reader.ReadBytes('\n')
				currentOffset += int64(len(line))
				if err != nil {
					if err == io.EOF {
						break
					}
					errors <- fmt.Errorf("error reading file: %v", err)
					return
				}
				hll.Add(line)
			}

			results <- &hll
		}(startOffset, endOffset)
	}

	// Aggregate results
	var mergedHLL *HyperLogLog
	for i := 0; i < numCores; i++ {
		select {
		case hll := <-results:
			if mergedHLL == nil {
				mergedHLL = hll
			} else {
				if err := mergedHLL.Merge(hll); err != nil {
					return nil, fmt.Errorf("error merging HyperLogLogs: %v", err)
				}
			}
		case err := <-errors:
			return nil, err
		}
	}

	return mergedHLL, nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: go run main.go <file_path>")
		return
	}

	precision := uint(16)

	// Process the file concurrently
	hll, err := processFileConcurrently(os.Args[1], precision)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	// Print the estimated count of unique rows
	fmt.Printf("Estimated number of unique rows: %d\n", hll.Count())
}
