package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/gocql/gocql"
	_ "github.com/meirf/gopart"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
)

var rdb = redis.NewClient(&redis.Options{
	Addr:     "localhost:6379",
	Password: "", // no password set
	DB:       1,  // use default DB
})
var ctx = context.Background()

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

func main() {

	cluster := gocql.NewCluster("127.0.0.1:9042")
	cluster.Keyspace = "singapore"
	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()
	defer session.Close()
	//err :=
	//session.Query(`SELECT part_id, lat_long, score FROM acs WHERE timeline = ? LIMIT 1`,
	//	"me").WithContext(ctx).Consistency(gocql.One).
	//err := session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`,
	//	"me").WithContext(ctx).Consistency(gocql.One).Scan(&id, &text)
	//err := session.Query("select lat_long from acs where pk1 = ?", "28000")

	//scanner := session.Query(`select lat_long from acs where part_id = ?`,
	//	"28000").WithContext(ctx).Iter().Scanner()
	//for scanner.Next() {
	var (
		//id    gocql.UUID
		score float32
	)
	if err := session.Query(`SELECT score FROM acs  `).Consistency(gocql.One).Scan(&score); err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tweet:", score)
	//}
	// scanner.Err() closes the iterator, so scanner nor iter should be used afterwards.
	//if err := scanner.Err(); err != nil {
	//	log.Fatal(err)
	//}
	//if err := session.Query(`INSERT INTO tweet (timeline, id, text) VALUES (?, ?, ?)`,
	//	"me", gocql.TimeUUID(), "hello world").Exec(); err != nil {
	//	log.Fatal(err)
	//}
	//
	//var id gocql.UUID
	//var text string
	//
	//if err := session.Query(`SELECT id, text FROM tweet WHERE timeline = ? LIMIT 1`,
	//	"me").Consistency(gocql.One).Scan(&id, &text); err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println("Tweet:", id, text)
	//
	//iter := session.Query(`SELECT id, text FROM tweet WHERE timeline = ?`, "me").Iter()
	//for iter.Scan(&id, &text) {
	//	fmt.Println("Tweet:", id, text)
	//}
	//if err := iter.Close(); err != nil {
	//	log.Fatal(err)
	//}

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
	//for _, item := range data.USCity {
	//	fmt.Printf("Name: item.usstate: %s \n", item.Usstate)
	//}

	//sort
	sort.Slice(data.USCity, func(i, j int) bool {
		return data.USCity[i].Score < data.USCity[j].Score
	})

	const partitionSize = 9000

	var count = 0
	for idxRange := range Partition(len(data.USCity), partitionSize) {
		count++
		bulkOperation(data.USCity[idxRange.Low:idxRange.High], idxRange.Low, count)
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

func bulkOperation(x []USCity, low int, count int) {
	//fmt.Println(x)
	sLow := strconv.Itoa(low)
	res, err := rdb.Do(ctx, "ACL", "SETUSER", ""+strconv.Itoa(count), "on", "~"+sLow, "+get", ">alanpassword").Result()
	if err != nil {
		//panic("everything ok, nil found")
		fmt.Println(err)
	}
	fmt.Printf("res: %s", res)
	for _, item := range x {
		//sLow := fmt.Sprintf("%f", low)
		//fmt.Printf(sLow)
		rdb.ZAdd(ctx, sLow, &redis.Z{Score: float64(item.Score), Member: item.getLngLat()})
		//rdb.ZAdd(ctx, sLow, &redis.Z{Score: float64(item.Score), Member: sLow})
	}

}
