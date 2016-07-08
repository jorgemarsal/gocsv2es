package main

import (
	"flag"
	"gopkg.in/olivere/elastic.v3"
	"encoding/csv"
	"io"
	"fmt"
	"os"
	"strings"
)

var esUrl = flag.String("esurl", "http://localhost:9200", "ElasticSearch URL.")
var csvFile = flag.String("csvfile", "", "CSV file.")
var fieldsStr = flag.String("fields", "", "Comma-separated list of column names in the CSV file. If not specified column names are inferred from the 1st row.")
var docsPerBatch = flag.Int("docsperbatch", 10000, "Number of documents per batch.")
var index = flag.String("index", "", "ElasticSearch index name.")
var docType = flag.String("doctype", "", "ElasticSearch document type.")

func main() {
	flag.Parse()

	fields := strings.Split(*fieldsStr, ",")

	fileReader, err := os.Open(*csvFile)
	if err != nil {
		// Handle error
		panic(err)
	}

	r := csv.NewReader(fileReader)

	client, err := elastic.NewClient(
		elastic.SetURL(*esUrl),
	)
	if err != nil {
		// Handle error
		panic(err)
	}

	_, err = client.CreateIndex(*index).Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	bulkRequest := client.Bulk()

	docs := 0
	total := 0
	malformed := 0

	if *fieldsStr == "" {
		fields, err = r.Read()
		fmt.Println("Column names: ", fields)
		if err != nil {
			// log.Fatal(err)
			malformed += 1
		}
	}

	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			malformed += 1
		}

		doc := map[string]string{}
		if len(record) != len(fields) {
			continue
		}
		for i, field := range fields {
			doc[field] = record[i]
		}

		bulkRequest = bulkRequest.Add(elastic.NewBulkIndexRequest().Index(*index).Type(*docType).Doc(doc))

		docs += 1
		if docs == *docsPerBatch {
			total += docs
			fmt.Printf("Sent %d docs\n", total)
			docs = 0
			_, err = bulkRequest.Do()
			if err != nil {
				// Handle error
				panic(err)
			}
		}
	}

	if docs > 0 {
		total += docs
		_, err = bulkRequest.Do()
		if err != nil {
			// Handle error
			panic(err)
		}

	}

	_, err = client.Refresh(*index).Do()
	if err != nil {
		// Handle error
		panic(err)
	}

	fmt.Printf("Sent %d docs\n", total)
	fmt.Printf("%d docs malformed\n", malformed)
}
