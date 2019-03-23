package main

import (
  "net"
  "fmt"
  "strings"
  "bytes"
  "time"
  "sync"
  "github.com/gomodule/redigo/redis"
  "os"
  "os/signal"
  "syscall"
  "runtime/pprof"
)

const (
    PollInterval = 100
    QueueLength = 999
)


func newPool() *redis.Pool {
    return &redis.Pool{
    // Other pool configuration not shown in this example.
    Dial: func () (redis.Conn, error) {
      c, err := redis.Dial("tcp", "127.0.0.1:6379")
      if err != nil {
        fmt.Println("Redis Connection failed!")
        return nil, err
      }
      if _, err := c.Do("AUTH", "jejakredis1234!"); err != nil {
        c.Close()
        return nil, err
      }
      return c, nil
    },
  }
}

var redisPool = newPool()





func handleConnection(conn net.Conn, waitGroup *sync.WaitGroup) {
    
    redisConn := redisPool.Get()
    defer redisConn.Close()
    defer conn.Close()
	defer waitGroup.Done()
    var failcount int
    timeoutDuration := 120 * time.Second
	ticker := time.NewTicker(PollInterval * time.Millisecond)
    for t := range ticker.C {
        //fmt.Println("Tick at", t)
        _ = t
        incomingData := make([]byte, 116) //make slice with type byte

        conn.SetReadDeadline(time.Now().Add(timeoutDuration))
        dataLength, err := conn.Read(incomingData)
        if err != nil {
            break
        }
        if dataLength == 0 {
            break
        } 
        rawData := string(incomingData[:dataLength]) //convert incoming byte to string
        
        //fmt.Println(conn.RemoteAddr().String())

        // check data length. accept only 108 and 116
        if dataLength == 116 || dataLength == 108 {
            
            rawDataSplit := strings.SplitN(rawData, "|", 2)
            deviceData, vehicleData := rawDataSplit[0], rawDataSplit[1]
            veh_datatype := deviceData[6:8]
            dev_imei := deviceData[8:]

            veh_serial := vehicleData[len(vehicleData)-6:len(vehicleData)-2] //take first 4 chars from last 6 chars

            data_checksum := rawData[len(rawData)-2:]
            our_checksum := calculateChecksum(rawData[0:len(rawData)-2]);

            if our_checksum != data_checksum {
                //fmt.Println("Incorrect data checksum")
                //conn.Close()
                failcount = failcount + 1
                continue
            }

            if strings.HasPrefix(veh_datatype, "E") || strings.HasPrefix(veh_datatype, "B"){
                //fmt.Println("Ignore this data type: " + veh_datatype)
                //conn.Close()
                failcount = failcount + 1
                continue
            } else {
                veh_datatype = "AA"
            }

            //generate ack
            ack := generateAck(veh_datatype, veh_serial)
            
            // send ack as a response
            _, err := conn.Write([]byte(ack))
            if err != nil { 
                fmt.Println("ACK NOT SENT: "+ dev_imei)
            }else{
                failcount = 0
                fmt.Println("Data sent to queue: " + dev_imei)
                redisConn.Do("LPUSH", "Q"+dev_imei, vehicleData)
                redisConn.Do("LTRIM", "Q"+dev_imei, 0, QueueLength)
            }

        } else {
            fmt.Println("Incorrect data length")
            
            //conn.Close()
            failcount = failcount + 1
            continue
        }
        
        if failcount > 30 {
            fmt.Println("To many failures! Closing connection..")
            break
        }
        
    }
    
    //conn.Close()
}


func generateAck(veh_datatype, veh_serial string) (ack string) {
   
    // if checksum ok, generate ACK

    var buffer bytes.Buffer
    buffer.WriteString("$$0014")
    buffer.WriteString(veh_datatype)
    buffer.WriteString(veh_serial)
    buffer.WriteString(calculateChecksum(buffer.String()))
    return buffer.String()

}

func calculateChecksum(rawData string) (hex string){
    var dec rune
    
    rawInAscii := []rune(rawData[0:])

	for i := range(rawInAscii) {
		dec ^= rawInAscii[i] //perform bitwise XOR
	}
    
	hex = fmt.Sprintf("%02X", dec) //convert dec to hex

    return hex
    
}

func cleanup() {
    
}

func main() {
    
    //DETECT SIGTERM
    c := make(chan os.Signal)
    signal.Notify(c, os.Interrupt, syscall.SIGTERM)
    go func() {
        <-c
        fmt.Println("cleanup")
        pprof.StopCPUProfile()
        os.Exit(1)
    }()

    //TRACE STARTS

    pprof.StartCPUProfile(os.Stdout)
	defer pprof.StopCPUProfile()



    //TRACE ENDS
	// Create a wait group to manage the goroutines.
	var waitGroup sync.WaitGroup

	// Perform 10 concurrent queries.
	
    
    
    ln, err := net.Listen("tcp", ":7007")
    if err != nil {
        fmt.Println("TCP Connection failed!")
    }

    // write_db_chan := make(chan string)
    for {
        conn, err := ln.Accept()
        if err != nil {
            // handle error
            continue
        }
        waitGroup.Add(1)
        go handleConnection(conn, &waitGroup)
        
    }
    waitGroup.Wait()

    
}


