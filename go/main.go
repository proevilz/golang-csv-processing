package main

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Row struct {
	Id        string
	FirstName string
	LastName  string
	Email     string
	Address   string
	City      string
	Country   string
}

const numWorkers = 10 // The number of worker goroutines to use

func main() {
	start := time.Now()
	log.Println("Starting CSV processing...")

	inputFile, err := os.Open("data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer inputFile.Close()

	inputReader := csv.NewReader(inputFile)

	outputFile, err := os.Open("output.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer outputFile.Close()

	outputWriter := csv.NewWriter(outputFile)

	// Read and discard the header row
	if _, err := inputReader.Read(); err != nil {
		log.Println(err)
	}

	// Create a wait group to synchronize the worker goroutines
	var wg sync.WaitGroup

	// Create a mutex to synchronize access to the shared output writer
	var outputWriterMutex sync.Mutex

	// Create a channel to send work to the worker goroutines
	workChan := make(chan *Row)

	// Launch the worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(&wg, workChan, &outputWriterMutex, outputWriter)
	}

	// Read the CSV file and send rows to the worker goroutines
	rowsProcessed := 0
	for {
		row, err := inputReader.Read()
		if err == io.EOF {
			// End of file
			break
		}
		if err != nil {
			log.Println(err)
			continue
		}

		// Convert the CSV row to a Row struct
		r := &Row{
			Id:        row[0],
			FirstName: row[1],
			LastName:  row[2],
			Email:     row[3],
			Address:   row[4],
			City:      row[5],
			Country:   row[6],
		}

		// Send the Row struct to the work channel
		workChan <- r
		rowsProcessed++
	}

	// Close the work channel to signal that there is no more work
	close(workChan)

	// Wait for all worker goroutines to complete before exiting the program
	wg.Wait()

	// Flush the output writer and close the output file
	outputWriter.Flush()

	end := time.Now()
	log.Printf("Processed %d rows in %s", rowsProcessed, end.Sub(start))
	log.Println("CSV processing complete.")
}

func worker(wg *sync.WaitGroup, workChan chan *Row, outputWriterMutex *sync.Mutex, outputWriter *csv.Writer) {
	defer wg.Done()

	// Process Rows from the work channel until it is closed
	for r := range workChan {

		// Modify the Id field of the Row struct
		r.Id = uuid.New().String()

		// Convert the Row struct back to a CSV row
		outputRow := []string{
			r.Id,
			r.FirstName,
			r.LastName,
			r.Email,
			r.Address,
			r.City,
			r.Country,
		}

		// Write the output row to the output writer
		outputWriterMutex.Lock()
		outputWriter.Write(outputRow)
		outputWriterMutex.Unlock()

	}
}
