package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
)

type Data struct {
	USCity []USCity `json:"uscity"`
}

type USCity struct {
	Usstate   string  `json:"usstate"` // .csv column headers
	Score     int     `json:"score"`
	Longitude float64 `json:"lng"`
	Latitude  float64 `json:"lat"`
}

func main() {
	filename := "/Users/mac/GolandProjects/rangePartitionLatLng/usss.json"
	jsonFile, err := os.Open(filename)
	if err != nil {
		fmt.Printf("failed to open json file: %s, error: %v", filename, err)
		return
	}
	defer jsonFile.Close()

	jsonData, err := ioutil.ReadAll(jsonFile)
	if err != nil {
		fmt.Printf("failed to read json file, error: %v", err)
		return
	}

	data := Data{}
	if err := json.Unmarshal(jsonData, &data); err != nil {
		fmt.Printf("failed to unmarshal json file, error: %v", err)
		return
	}

	// Print
	for _, item := range data.USCity {
		fmt.Printf("Name: item.usstate: %s \n", item.Usstate)
	}
}
