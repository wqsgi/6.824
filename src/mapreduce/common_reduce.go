package mapreduce

import (
	"path"
	"strconv"
	"bufio"
	"os"
	"io"
	"strings"
	"encoding/json"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
var reduceDir = "D:\\mapreduce\\reduce"
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	for i:=0;i<nMap;i++{
		reduceMap:=make(map[string][]string)
		mapPath:=path.Join(mapDir,jobName+"_m_"+strconv.Itoa(i)+"_"+strconv.Itoa(reduceTaskNumber))
		os.Remove(mapPath)
		f,err:=os.Open(mapPath)
		if err != nil{
			panic(err)
		}
		reader:=bufio.NewReader(f)
		for {
			line,_,err:=reader.ReadLine()
			if err == io.EOF{
				break
			}
			if err != nil && err != io.EOF{
				return
			}
			arrays:=strings.Split(string(line)," ")

			if _,ok:=reduceMap[arrays[0]];!ok{
				var values []string
				reduceMap[arrays[0]]=values
			}
			reduceMap[arrays[0]]=append(reduceMap[arrays[0]],arrays[1])

		}
		reduceFile,err:=os.OpenFile(path.Join(reduceDir,"mrtmp."+jobName+"-res-"+strconv.Itoa(reduceTaskNumber)),
			os.O_RDWR|os.O_CREATE,777)
		if err!= nil{
			panic(err)
		}
		defer reduceFile.Close()
		for key,value:=range reduceMap{
			ret:=reduceF(key,value)
			kv,_:=json.Marshal(&KeyValue{Key:key,Value:ret})
			reduceFile.Write([]byte(kv))
			reduceFile.Write([]byte("\r\n"))
		}
	}






	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
}
