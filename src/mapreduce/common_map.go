package mapreduce

import (
	"hash/fnv"
	"os"
	"bufio"
	"io"
	"strconv"
	"path"
	"fmt"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
var mapDir = "D:\\mapreduce\\map"
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {

	println(mapTaskNumber)
	file,err:=os.Open(inFile)
	if err!=nil{
		println(err)
		return
	}
	defer file.Close()
	reader:=bufio.NewReader(file)
	fileDir:=mapDir
	var files []*os.File
	for i:=0;i<nReduce;i++{

		file:=path.Join(fileDir,"\\"+jobName+"_m_"+strconv.Itoa(mapTaskNumber)+"_"+strconv.Itoa(i))
		if _,err:=os.Stat(file);err!=nil{
			os.Create(file)
		}
		os.Remove(file)
		if f,err:=os.OpenFile(file,os.O_RDWR|os.O_CREATE,777);err==nil{
			defer f.Close()
			files=append(files,f)
		} else {
			fmt.Println(fileDir)
		}

	}

	for {
		line,_,err:=reader.ReadLine()
		if err==io.EOF{
			fmt.Println(err)
			break
		}

		kvs:=mapF(inFile,string(line))
		for _,kv:=range kvs{
			fileNum:=uint32(ihash(kv.Key))%uint32(nReduce)
			ret:=kv.Key+" "+kv.Value+"\r\n"
			files[fileNum].Write([]byte(ret))

		}

	}


	// TODO:
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
