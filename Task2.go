package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

const nGoroutines uint32 = 12

func factorize(n uint32) uint32 {
	var count uint32 = 0
	for n%2 == 0 {
		count += 1
		n >>= 1
	}
	nSqrt := uint32(math.Sqrt(float64(n)))
	for i := uint32(3); i <= nSqrt; i += 2 {
		for n%i == 0 {
			n /= i
			count += 1
		}
	}

	if n > 2 {
		count += 1
	}
	return count
}

func timeStats(times [10]int64) (float64, float64) {
	var mean float64
	for _, t := range times {
		mean += float64(t)
	}
	mean /= float64(len(times))

	var std float64
	for _, t := range times {
		std += math.Pow(float64(t)-mean, 2)
	}
	std = math.Sqrt(std / float64(len(times)))
	return mean, std
}

func factorizeNumbers(inChannel <-chan uint32, outChannel chan<- uint32, wg *sync.WaitGroup) {
	defer close(outChannel)
	defer wg.Done()

	var localSum uint32
	for {
		n, opened := <-inChannel
		if !opened {
			break
		}
		localSum += factorize(n)
	}
	outChannel <- localSum
}

func sendNumbersToChannels(scanner *bufio.Scanner, chans [nGoroutines]chan uint32) {
	for i := 0; i < len(chans); i++ {
		defer close((chans)[i])
	}

	i := uint32(0)
	for scanner.Scan() {
		currValue, err := strconv.Atoi(scanner.Text())
		if err != nil {
			break
		} else {
			currValue := uint32(currValue)
			chans[i%nGoroutines] <- currValue
		}
		i += 1
	}
}

func getSumsFromChannels(chans [nGoroutines]chan uint32) uint32 {
	var totalSum uint32
	for i := uint32(0); i < nGoroutines; i++ {
		localSum, _ := <-chans[i] // получаем данные из потока
		totalSum += localSum
	}
	return totalSum
}

func useGoroutines() uint32 {
	file, err := os.Open("numbers.txt")

	if err != nil {
		log.Fatal(err)
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)
	var wg sync.WaitGroup

	var inChans [nGoroutines]chan uint32
	var outChans [nGoroutines]chan uint32
	wg.Add(int(nGoroutines))
	for i := uint32(0); i < nGoroutines; i++ {
		inChans[i] = make(chan uint32)
		outChans[i] = make(chan uint32)
		go factorizeNumbers(inChans[i], outChans[i], &wg)
	}

	scanner := bufio.NewScanner(file)
	sendNumbersToChannels(scanner, inChans)

	totalSum := getSumsFromChannels(outChans)
	wg.Wait()
	return totalSum

}

func main() {

	var times [10]int64
	var start time.Time
	var totalSum uint32

	for i := 0; i < 10; i++ {
		start = time.Now()
		totalSum = useGoroutines()
		times[i] = time.Since(start).Milliseconds()
	}

	mean, std := timeStats(times)
	fmt.Println("Goroutines")
	fmt.Println(fmt.Sprintf("Time: %.2fms mean %.2fms std", mean, std))
	fmt.Println("Result:", totalSum)

}
