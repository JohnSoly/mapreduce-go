package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"os"
	"sort"
)

func runMapTask(
	jobName string, // The name of the whole mapreduce job
	mapTaskIndex int, // The index of the map task
	inputFile string, // The path to the input file that was assigned to this task
	nReduce int, // The number of reduce tasks
	mapFn func(file string, contents string) []KeyValue, // The user-defined map function
) {
	content, err := os.ReadFile(inputFile)
	if err != nil {
		log.Fatalf("Error reading file %s: %v", inputFile, err)
	}
	keyValuePairs := mapFn(inputFile, string(content))
	interMidFileRef := make([]*os.File, nReduce)
	jsonEncoder := make([]*json.Encoder, nReduce)

	for i := 0; i < nReduce; i++ {
		interMidfileName := getIntermediateName(jobName, mapTaskIndex, i)
		interMidfile, err := os.Create(interMidfileName)
		if err != nil {
			log.Fatalf("Error  creating InterMidFile %s: %v", interMidfileName, err)
		}
		interMidFileRef[i] = interMidfile
		jsonEncoder[i] = json.NewEncoder(interMidfile)

	}
	for i := 0; i < len(keyValuePairs); i++ {
		index := int(hash32(keyValuePairs[i].Key) % uint32(nReduce)) // figuring out which intermid file should handle the pair
		err := jsonEncoder[index].Encode(&keyValuePairs[i])          //write the pair in the correct file
		if err != nil {
			log.Fatalf("Error  writing into InterMidFile: %v", err)
		}

	}
	for _, file := range interMidFileRef {
		file.Close()
	}

}

func hash32(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func runReduceTask(
	jobName string, // the name of the whole MapReduce job
	reduceTaskIndex int, // the index of the reduce task
	nMap int, // the number of map tasks
	reduceFn func(key string, values []string) string,
) {

	KeyValuesMap := make(map[string][]string)

	for i := 0; i < nMap; i++ {
		interMidfileName := getIntermediateName(jobName, i, reduceTaskIndex)
		interMidfile, err := os.Open(interMidfileName)
		if err != nil {
			log.Fatalf("Error opening InterMidFile %s: %v", interMidfileName, err)
		}
		defer interMidfile.Close()

		decoder := json.NewDecoder(interMidfile)
		var KV KeyValue
		for decoder.Decode(&KV) == nil {
			KeyValuesMap[KV.Key] = append(KeyValuesMap[KV.Key], KV.Value)
		}
	}
	var sortedKeys []string
	for key := range KeyValuesMap {
		sortedKeys = append(sortedKeys, key)
	}
	sort.Strings(sortedKeys)

	OPfileName := getReduceOutName(jobName, reduceTaskIndex)
	OPfile, err := os.Create(OPfileName)
	if err != nil {
		log.Fatalf("Error creating output file %s: %v", OPfileName, err)
	}
	defer OPfile.Close()

	jsonEncoder := json.NewEncoder(OPfile)
	for key, values := range KeyValuesMap {
		SumValue := reduceFn(key, values)
		err := jsonEncoder.Encode(KeyValue{Key: key, Value: SumValue})
		if err != nil {
			log.Fatalf("Error writing to output file: %v", err)
		}
	}
}
