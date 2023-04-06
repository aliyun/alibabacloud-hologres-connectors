package main

import (
	"fmt"
	holoclient "holoclient/holo-client"
	"math/rand"
	"strconv"
	"time"
)

func simpleGetTest() {
	connInfo := "host=xxxxx port=xxx dbname=xxxx user=xxxxx password=xxxxx"
	config := holoclient.NewHoloConfig(connInfo)
	client := holoclient.NewHoloClient(config)
	//create table test_get(id int, name text, address text, primary key(id));
	schema := client.GetTableschema("", "test_get", false)

	for i := 0; i < 100; i++ {
		get := holoclient.NewGetRequest(schema)
		get.SetGetValByColIndex(0, strconv.Itoa(i), len(strconv.Itoa(i)))
		res := client.Get(get)
		fmt.Printf("Record %d:\n", i)
		if res == nil {
			fmt.Println("No record")
		} else {
			for col := 0; col < schema.NumColumns(); col++ {
				col_val := res.GetVal(col)
				fmt.Println(col_val)
			}
		}
		get.DestroyGet()
	}

	client.Flush()
	client.Close()
}

func getListTest() {
	connInfo := "host=xxxxx port=xxx dbname=xxxx user=xxxxx password=xxxxx"
	config := holoclient.NewHoloConfig(connInfo)
	client := holoclient.NewHoloClient(config)
	schema := client.GetTableschema("", "test_get", false)

	getList := make([]*holoclient.GetRequest, 100)
	for i := 0; i < 100; i++ {
		getList[i] = holoclient.NewGetRequest(schema)
		getList[i].SetGetValByColIndex(0, strconv.Itoa(i), len(strconv.Itoa(i)))
	}
	recordList := client.GetList(getList)
	for i := 0; i < 100; i++ {
		fmt.Printf("Record %d:\n", i)
		if recordList[i] == nil {
			fmt.Println("No record")
		} else {
			for col := 0; col < schema.NumColumns(); col++ {
				col_val := recordList[i].GetVal(col)
				fmt.Println(col_val)
			}
		}
		getList[i].DestroyGet()
	}

	client.Flush()
	client.Close()
}

func simplePutTest() {
	connInfo := "host=xxxxx port=xxx dbname=xxxx user=xxxxx password=xxxxx"
	config := holoclient.NewHoloConfig(connInfo)
	config.SetWriteMode(holoclient.INSERT_OR_REPLACE)
	client := holoclient.NewHoloClient(config)
	//create table test_go_put (id int, f1 real, f2 boolean, f3 text, f4 int[], f5 boolean[], f6 real[], f7 float[], primary key(id));
	schema := client.GetTableschema("", "test_go_put", false)

	intArray := []int32{3, 4, 5, 6}
	boolArray := []bool{true, false, true}
	floatArray := []float32{1.123, 2.234}
	doubleArray := []float64{1.2345, 3.4567}
	for i := 0; i < 100; i++ {
		put := holoclient.NewMutationRequest(schema)
		put.SetInt32ValByColIndex(0, int32(i))
		put.SetFloat32ValByColIndex(1, 123.234)
		put.SetBoolValByColIndex(2, true)
		put.SetTextValByColIndex(3, "hello", 5)
		put.SetInt32ArrayValByColIndex(4, intArray)
		put.SetBoolArrayValByColIndex(5, boolArray)
		put.SetFloat32ArrayValByColIndex(6, floatArray)
		put.SetFloat64ArrayValByColIndex(7, doubleArray)
		client.Submit(put)
	}

	client.Flush()
	client.Close()
}

func getInMultiThread() {
	connInfo := "host=xxxxx port=xxx dbname=xxxx user=xxxxx password=xxxxx"
	config := holoclient.NewHoloConfig(connInfo)
	config.SetThreadSize(20)
	numThread := 100
	client := holoclient.NewHoloClient(config)
	schema := client.GetTableschema("", "test_get", false)
	running := true
	numRecord := make(chan int)
	f := func() {
		num := 0
		for running {
			pk := rand.Intn(100000000)
			get := holoclient.NewGetRequest(schema)
			get.SetGetValByColIndex(0, strconv.Itoa(pk), len(strconv.Itoa(pk)))
			res := client.Get(get)
			if res == nil {
				fmt.Println("No record")
			}
			get.DestroyGet()
			num++
		}
		fmt.Println(num)
		numRecord <- num
	}
	for i := 0; i < numThread; i++ {
		go f()
	}
	time.Sleep(60 * time.Second)
	running = false
	sum := 0
	for i := 0; i < numThread; i++ {
		curnum := <-numRecord
		sum = sum + curnum
	}
	totalRps := float32(sum) / 60.0
	fmt.Printf("total records: %d\n", sum)
	fmt.Printf("total Rps: %f\n", totalRps)

	client.Flush()
	client.Close()
}

func main() {
	holoclient.HoloClientLoggerOpen() //HoloClientLoggerOpen()和HoloClientLoggerClose()只需要全局调用一次
	simpleGetTest()
	getListTest()
	simplePutTest()
	getInMultiThread()
	holoclient.HoloClientLoggerClose()
}
