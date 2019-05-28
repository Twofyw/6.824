package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	"sort"
)

// ByKey implements sort.Interface for []KeyValue based on
// the Key field.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	//
	// doReduce manages one reduce task: it should
	// read the intermediate files for the task
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	var interm []KeyValue
	for m := 0; m < nMap; m++ {
		intermFilename := reduceName(jobName, m, reduceTask)
		f, err := os.Open(intermFilename)
		checkError(err)
		// Your doMap() encoded the key/value pairs in the intermediate
		// files, so you will need to decode them. If you used JSON, you can
		// read and decode by creating a decoder and repeatedly calling
		// .Decode(&kv) on it until it returns an error.
		dec := json.NewDecoder(f)
		var kv KeyValue
		for err = dec.Decode(&kv); err == nil; err = dec.Decode(&kv) {
			interm = append(interm, kv)
		}
		f.Close()
	}

	// sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	sort.Slice(interm, func(i, j int) bool {
		return interm[i].Key < interm[j].Key
	})

	reduceFilename := mergeName(jobName, reduceTask)
	fmt.Println("reduceFilename", reduceFilename)
	// f, err := os.OpenFile(reduceFilename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	f, err := os.Create(reduceFilename)
	checkError(err)
	enc := json.NewEncoder(f)

	// batch keys and output
	currentKey := interm[0].Key
	var aValues []string
	for i := 0; i < len(interm); i++ {
		if interm[i].Key == currentKey {
			// fmt.Println("same key", interm[i].Key)
			aValues = append(aValues, interm[i].Value)
		} else {
			enc.Encode(KeyValue{currentKey, reduceF(currentKey, aValues)})
			currentKey = interm[i].Key
			aValues = nil
			aValues = append(aValues, interm[i].Value)
		}
	}
	// output last key
	enc.Encode(KeyValue{currentKey, reduceF(currentKey, aValues)})
	f.Close()

	//
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
