package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	_ "github.com/meirf/gopart"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

//type IUSCity interface {
//	getUSCity() USCity
//}

func (usc USCity) getUSCity() string {
	return usc.Usstate + "," + strconv.FormatInt(int64(usc.Score), 10) + "," + strconv.FormatFloat(usc.Longitude, 'E', -1, 64) + "," + strconv.FormatFloat(usc.Latitude, 'E', -1, 64)
}
func (usc USCity) getLngLat() string {
	//if s, err := strconv.ParseFloat(usc.Longitude, 64); err == nil {
	//	fmt.Println(s) // 3.14159265
	//}
	return fmt.Sprintf("%g", usc.Longitude) + "," + fmt.Sprintf("%g", usc.Latitude)
}

var ctx = context.Background()

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

	//sort
	sort.Slice(data.USCity, func(i, j int) bool {
		return data.USCity[i].Score < data.USCity[j].Score
	})

	const partitionSize = 2000
	for idxRange := range Partition(len(data.USCity), partitionSize) {
		bulkOperation(data.USCity[idxRange.Low:idxRange.High], idxRange.Low)
	}

	//for _, item := range data.USCity {
	//	fmt.Printf("Name: item.usstate: %s \n", item.Score)
	//}
}

type IdxRange struct {
	Low, High int
}

func Partition(collectionLen, partitionSize int) chan IdxRange {
	c := make(chan IdxRange)
	if partitionSize <= 0 {
		close(c)
		return c
	}

	go func() {
		numFullPartitions := collectionLen / partitionSize
		fmt.Println("numFullPartitions: %s", numFullPartitions)

		var i int
		for ; i < numFullPartitions; i++ {
			c <- IdxRange{Low: i * partitionSize, High: (i + 1) * partitionSize}

		}

		if collectionLen%partitionSize != 0 { // left over
			c <- IdxRange{Low: i * partitionSize, High: collectionLen}
		}

		close(c)
	}()
	return c
}

func bulkOperation(x []USCity, low int) {
	//fmt.Println(x)
	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       1,  // use default DB
	})
	ctx := context.Background()
	for _, item := range x {
		//sLow := fmt.Sprintf("%f", low)
		sLow := strconv.Itoa(low)
		//fmt.Printf(sLow)
		rdb.ZAdd(ctx, sLow, &redis.Z{Score: float64(item.Score), Member: item.getLngLat()})
	}

}
