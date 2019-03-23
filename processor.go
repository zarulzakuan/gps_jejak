package main

import (
    "fmt"
    "github.com/gomodule/redigo/redis"
    "time"
    "encoding/json"
    "github.com/globalsign/mgo"
    "github.com/globalsign/mgo/bson"
    "strconv"
    "strings"
    "sync"

)


const (
	MongoDBHost_pri = "mongomaster:27017"
    MongoDBHost_sec = "mongoslave:27017" //we are not using this. yet.. I don't know, the connection keeps dropping
    mongoTimeout = 180 // 3 minutes
    Database = "jejakktmb"

    RedisServer = "127.0.0.1:6379"
    RedisAuth = "jejakredis1234!"

    PollInterval = 50
    CheckQueuePollInterval = 5000

    //DEBUG OUTPUT

    iWannaSeeThisIMEI = "861013034520941"

    ///ALGORITHM CONSTANT///

    maxRadiusToFindStation = 200 // meter
    maxDelayThatWeCanAccept = 900 // 15 minutes
    maxAngleDiffWeCanTolerate = 30 // degree
    maxIdlingWeCanAccept = 1200 // 20 minutes
)

type json_data struct {
    ID        bson.ObjectId `bson:"_id,omitempty"`
	DevIMEI        string `json:"dev_IMEI"`
	VehADC1        string `json:"veh_ADC1"`
	VehAngle       string `json:"veh_angle"`
	VehBattVoltage string `json:"veh_batt_voltage"`
	VehCellID      string `json:"veh_cellID"`
	VehDatetime    string `json:"veh_datetime"`
	VehDatetimeParsed    string `json:"veh_datetime_parsed"`
	VehEw          string `json:"veh_ew"`
	VehHdop        string `json:"veh_hdop"`
	VehLat         string `json:"veh_lat"`
	VehLoc         string `json:"veh_loc"`
	VehLong        string `json:"veh_long"`
	VehMileage     string `json:"veh_mileage"`
	VehNs          string `json:"veh_ns"`
	VehSat         string `json:"veh_sat"`
	VehSignal      string `json:"veh_signal"`
	VehSpeed       string `json:"veh_speed"`
	VehStatus      string `json:"veh_status"`
	VehSuppVoltage string `json:"veh_supp_voltage"`
	VehTemp1       string `json:"veh_temp1"`
    VehTemp2       string `json:"veh_temp2"`
    Lat            string `json:"lat"`
    Long           string `json:"long"`
    DistanceTravelled           string `json:"distance_travelled"`
    Timestamp time.Time 
    Delay           string `json:"delay"`
    TrainNum        string `json:"train_num"`
    IdleLength      string `json:"idle_length"`
    SetNumber      string `json:"set_num"`
}

type IMEI_Mapping struct {
    IMEI        int     `bson:"imei" json:"imei"`
    SetNumber   string  `bson:"setnum" json:"setnum"`
    CoachNumber string  `bson:"coachnum" json:"coachnum"`
    IMEIPair    int     `bson:"pair" json:"pair"`

}

type Stop struct {
	StationName         string  `bson:"stname" json:"stname"`
	StationId           int     `bson:"stid" json:"stid"`
	DistanceFromOrigin  int     `bson:"dist" json:"dist"`
}

type Timetable struct {
    TrainNum    int `bson:"trainnum" json:"trainnum"`
    Arrival  int `bson:"arrival" json:"arrival"`
    ArrivalSec  int `bson:"arrivalsec" json:"arrivalsec"`
    Diff        int `bson:"diff" json:"diff"`
    Diff2        int `bson:"diff2" json:"diff2"`

}

func newPool() *redis.Pool {
    return &redis.Pool{
    // Other pool configuration not shown in this example.
    Dial: func () (redis.Conn, error) {
      c, err := redis.Dial("tcp", RedisServer)
      if err != nil {
        fmt.Println("Cannot connect Redis -", err)
        return nil, err
      }
      if _, err := c.Do("AUTH", RedisAuth); err != nil {
        c.Close()
        return nil, err
      }
      return c, nil
    },
  }
}

// Global vars
var redisPool = newPool()

type state_table struct{
    trainnum            int
    laststop            int
    arrivalsec          int
    distancetravelled   int
    actual_arrivalsec   int
    delay               int
    azimuth             int
    speed               int
    idlinglength        int
    idlingtimesec       int
}
var state = map[string]state_table{}

type train_delay_table struct{
    imei            string
    delay           int
    angle           int
}
var train_delay = map[string]train_delay_table{}


var mutex = &sync.Mutex{}


func main() {
    
    //setup Redis
    redisConn := redisPool.Get()
    defer redisConn.Close()

    //setup mongo
    // We need this object to establish a session to our MongoDB.
    mongoDBDialInfo := &mgo.DialInfo{
        Addrs:    []string{MongoDBHost_pri},
        Timeout:  mongoTimeout * time.Second,
    }

    // Create a session which maintains a pool of socket connections
    // to our MongoDB.
    mongoSession, err := mgo.DialWithInfo(mongoDBDialInfo)
    if err != nil {
        fmt.Println("Cannot connect to MongoDB - ", err)
        panic(err)
    }

    mongoSession.SetMode(mgo.Monotonic, true)

    // Create a wait group to manage the goroutines.
    var waitGroup sync.WaitGroup
    //var savedQueues []string
    for {
        
        
        //get all available queues

        queues, err := redis.Values(redisConn.Do("KEYS", "Q*"))
        //redisConn.Close()
        //fmt.Println("Found ",len(queues)," new queues")
        if err != nil {
            fmt.Println("Failed to get data from queue:", err)
        }
        
        for _, q := range queues {
            // if (Contains(savedQueues, fmt.Sprintf("%s", q))) {
            //     fmt.Println("Exist, continue")
            //     continue
            // }
            // savedQueues = append(savedQueues, fmt.Sprintf("%s", q))
            //fmt.Println("Listening to queue:",  fmt.Sprintf("%s", q))
            waitGroup.Add(1)
            go readQueue(fmt.Sprintf("%s", q), mongoSession, &waitGroup)
            
        }
        //waitGroup.Wait()
        time.Sleep(CheckQueuePollInterval * time.Millisecond)
        //fmt.Println("Rechecking available queues")
        //fmt.Println("We have ", len(savedQueues), " queues" )
    }
        
}

func Contains(a []string, x string) bool {
	for _, n := range a {
		if x == n {
			return true
		}
	}
	return false
}

func readQueue(queue string, mongoSession *mgo.Session, waitGroup *sync.WaitGroup) {
    
    redisConn := redisPool.Get()
    defer redisConn.Close()
    sessionCopy := mongoSession.Copy()
    defer sessionCopy.Close()
    defer func() {
        fmt.Println("Finish queue: ", queue)
        waitGroup.Done()
    }()

    for {
        
        task, err := redis.Values(redisConn.Do("BRPOP", queue, 0))
        if err != nil {
            fmt.Println("Failed to get data from queue: ", err)
        }
        if len(task) != 2 {

        }else{
            imei := fmt.Sprintf("%s", task[0])
            data := fmt.Sprintf("%s", task[1])
            imei = strings.TrimPrefix(imei, "Q")
            //fmt.Println("Working on data: ",data)
            processData(imei, data, sessionCopy)

        }
        
        time.Sleep(PollInterval * time.Millisecond)


    }
    fmt.Println("Finish reading queue", queue, ". Closing connection")
}

// process data for each imei (this is where the ALGORITHM sits. *WARNING* this is very procedural because I'm no good in OOP. Better get your pencil and paper)
func processData(dev_imei string, vehicleData string, mongoSession *mgo.Session) {
    
    //setup redis and mongo connection session
    redisConn := redisPool.Get()
    sessionCopy := mongoSession.Copy()

    //defer
    defer redisConn.Close()
	defer sessionCopy.Close()

	// Get a collection to execute the query against.
    collection_pdata := sessionCopy.DB(Database).C("processed_data")  // we save any translated legit data to this collection
    collection_laststate_data := sessionCopy.DB(Database).C("laststate_data") // we save the device's last state to this collection
    collection_stops := sessionCopy.DB(Database).C("stops")  // we use this collection to identify which stop the device is on
    collection_timetable := sessionCopy.DB(Database).C("timetable") // we use this collection to identify which train number the device belongs to
    collection_imeimap := sessionCopy.DB(Database).C("imeimap") // we use this collection to identify which train set number based on imei

    // translate received data and save translated data in jsondata
    translated, jsondata := translateData(dev_imei, vehicleData)

    //convert json object to bytes so we can map it to go struct json_data
    bytes := []byte(jsondata)
    
    // create JSON object (valid inside this function only!) out of struct
    // JSON_data will be destroyed when this function ends
    var JSON_data json_data
    var imeiMap IMEI_Mapping
    // save current time
    JSON_data.Timestamp = time.Now()

    //find setnum

    dev_imei_int, err2 := strconv.Atoi(dev_imei)
    if err2 != nil {
        panic(err2)
    }
    var err = collection_imeimap.Find(bson.M{"imei": dev_imei_int}).One(&imeiMap)

    if err == mgo.ErrNotFound {
        //fmt.Println(dev_imei, ": Cannot recognize any setnum")
    }else{
       // fmt.Println("set: ", dev_imei, "<==>", imeiMap.SetNumber)

    }

    if translated == true { 

        //map translates json string (that was converted to bytes) to JSON_data object
        if err := json.Unmarshal(bytes, &JSON_data); err != nil {
            panic(err)
        }


        t := time.Now()
        localtz, _ := time.LoadLocation("Asia/Singapore")
        t = t.In(localtz)

        h := t.Hour()*60*60
        m := t.Minute()*60

        // set current time in seconds to use for searching train number in mongo
        nowtimesec := h+m
        //fmt.Println("TIME REFERENCE: ", nowtimesec)

        // find current stop 
        currentSpeed, _ := strconv.Atoi(JSON_data.VehSpeed)
        currentMileage,_ := strconv.Atoi(JSON_data.VehMileage)

        //get last state if this imei
        mutex.Lock()
        last_state_trainnum := state[dev_imei].trainnum
        last_state_laststop := state[dev_imei].laststop
        last_state_arrivalsec := state[dev_imei].arrivalsec
        last_state_actual_arrivalsec := state[dev_imei].actual_arrivalsec
        last_state_delay := state[dev_imei].delay
        last_state_distancetravelled := state[dev_imei].distancetravelled
        last_state_azimuth := state[dev_imei].azimuth
        last_speed := state[dev_imei].speed
        last_idling_length := state[dev_imei].idlinglength
        last_idling_time_sec := state[dev_imei].idlingtimesec
        mutex.Unlock()
        

        // calculate how long this train has travelled 
        /////////////////////////// TO DO /////////////////////////////////////
        // NOT DONE YET WE HAVENT DECIDE WHEN TO RESET COUNTER
        ///////////////////////////////////////////////////////////////////////
        if last_state_distancetravelled == 0 {
            last_state_distancetravelled = currentMileage
        }else{
            last_state_distancetravelled = last_state_distancetravelled + currentMileage
        }
        
        var currentStopInfo Stop // to hold stop info
        var timetableInfo Timetable // to hold timetable result with train num 
        if dev_imei == iWannaSeeThisIMEI {
            fmt.Println(dev_imei, ":disntance: ", last_state_distancetravelled, currentMileage)
        }
        // start calculating how long the train is idling if speed is 0
        if currentSpeed == 0 {

            if last_idling_time_sec == 0 {
                last_idling_time_sec = nowtimesec
            }

            if last_speed == 0 {
                //start incrementing idling length

                stopPeriod := nowtimesec - last_idling_time_sec 
                last_idling_time_sec = nowtimesec
                last_idling_length = last_idling_length + stopPeriod

                if dev_imei == iWannaSeeThisIMEI {
                    fmt.Println(dev_imei, ":has been stopping for: ", last_idling_length)
                }

                if last_idling_length > maxIdlingWeCanAccept {

                    mutex.Lock()
                    delete(state, dev_imei)
                    
                    if dev_imei == iWannaSeeThisIMEI {
                        fmt.Println(dev_imei, ":has been stopping for more than: ", maxIdlingWeCanAccept, "seconds which is", last_idling_length)
                        fmt.Println(dev_imei, ":Removing key from state map")
                        fmt.Println(dev_imei, ":Removing key from train_delay map")
                    }
                    
                    for k, _ := range train_delay { 
                        if train_delay[k].imei == dev_imei {
                            delete(train_delay, k)
                        }
                    }
                    mutex.Unlock()
                }
                
            }else{
                //start idling calculating
                last_idling_length = 0
                last_idling_time_sec = nowtimesec;
            }
            
        }else{
            //if current speed more than 0,
            if last_speed == 0 {
                //fmt.Println("Checking stop..", JSON_data.Long, JSON_data.Lat)
                //reset stop period
                last_idling_length = 0
                long,_ := strconv.ParseFloat(JSON_data.Long, 64)
                vehangle,_ := strconv.ParseFloat(JSON_data.VehAngle, 64)
                
                lat,_ := strconv.ParseFloat(JSON_data.Lat, 64)
                var current_coord = []float64{long,lat}
                //var tryNextTimetable bool = false 
                var skipThisTrainnum int = 0 
                var okToCheckAgain bool = true
                // first we find the stop id (train station id), because we need it to search the timetable
                // but for KL Sentral, as the station is underground, the train coordinate may not be accurate so we need wider maxDistance.
                // 
                copymaxRadiusToFindStation := maxRadiusToFindStation
                if last_state_laststop == 19000 || last_state_laststop == 19205 { // if last station was Kuala Lumpur or Mid Valley (these are the only 2 stations before and after KL Sentral)
                    copymaxRadiusToFindStation = 400 //meter

                }

                var err = collection_stops.Find(bson.M{"loc": bson.M{
                    "$near": bson.M{
                        "$geometry": bson.M{
                            "type": "Point",
                            "coordinates": current_coord},
                        "$maxDistance": copymaxRadiusToFindStation}}}).One(&currentStopInfo)

                if err == mgo.ErrNotFound {
                    //fmt.Println(dev_imei, ": Cannot recognize any station")
                }else{
                // OK we have found which station this device is in
                    //fmt.Println(dev_imei, ":We are at: ", currentStopInfo.StationName)
                    //now we only check timetable if the device was stop (speed was 0)
                    if dev_imei == iWannaSeeThisIMEI {
                        fmt.Println(dev_imei, ":last speed: ", last_speed, currentSpeed, currentStopInfo.StationName)
                    }

                }
                 //check timetable to find the train number
CHECKTIMETABLE:               

                /////START///// this thing here for searching train num from timetable. i dont want to explain. go figure
                type list []interface{}
                sub := list{nowtimesec, "$arrivalsec"}

                pipeline := []bson.M{ {"$match": bson.M {"stop" : currentStopInfo.StationId, "angle" : bson.M { "$type" : 16 } , "trainnum" : bson.M { "$ne" : skipThisTrainnum }} } , {"$project": bson.M{ "arrivalsec": 1, "angle": 1, "distance": 1, "arrival": 1, "trainnum": 1, "diff": bson.M{ "$abs": bson.M{ "$subtract": sub } }, "diff2": bson.M{ "$subtract" : []interface{}{180, bson.M{"$abs": bson.M{ "$subtract": []interface{}{ bson.M{"$abs": bson.M{ "$subtract": []interface{}{vehangle, "$angle"}}} , 180}}}} }  }}, {"$sort": bson.M{	"diff": 1, "diff2": 1  }}, {"$limit": 1} } 
                err =  collection_timetable.Pipe(pipeline).One(&timetableInfo)
                /////END///// 
                
                if err == mgo.ErrNotFound {
                    if dev_imei == iWannaSeeThisIMEI {
                        fmt.Println(dev_imei, ": NO TIMETABLE FOUND FOR", currentStopInfo.StationId)
                    }
                }else{
                    if err != nil {
                        panic(err)
                    }else{
                        // if we found the time table info
                        if dev_imei == iWannaSeeThisIMEI {
                            fmt.Println(dev_imei, ":FOUND TRAIN NUMBER: ", timetableInfo.TrainNum)
                        }
                        
                        delaySec :=  timetableInfo.Diff
                        angleDiff :=  timetableInfo.Diff2
                        

                        if dev_imei == iWannaSeeThisIMEI {
                            fmt.Println(dev_imei, ":DELAY: ", delaySec, "ANGLEDIFF:", angleDiff)
                        }

                        if delaySec > maxDelayThatWeCanAccept {
                            // this probably not true, so we ignore it
                        }else{

                            //now KL Sentral is underground station, the train angle might not be accurate so we need to accept wider angle diff
                            var KLSentralOK bool = false
                            if timetableInfo.TrainNum == 19100 {
                                if angleDiff < 90 {
                                    KLSentralOK = true
                                }
                            }

                            // check we're on the right direction
                            if angleDiff < maxAngleDiffWeCanTolerate || KLSentralOK {

                                mutex.Lock()
                                // check first if we already have the record
                                if _, ok := train_delay[strconv.Itoa(timetableInfo.TrainNum)]; ok {
                                    if dev_imei == iWannaSeeThisIMEI {
                                        fmt.Println(dev_imei, ":GOT RECORD: ", timetableInfo.TrainNum)
                                        //panic("meh")
                                    }
                                    //if already has and has the same imei, update it
                                    if train_delay[strconv.Itoa(timetableInfo.TrainNum)].imei == dev_imei {
                                        train_delay[strconv.Itoa(timetableInfo.TrainNum)] = train_delay_table{imei: dev_imei, delay: delaySec, angle: angleDiff}
                                        if dev_imei == iWannaSeeThisIMEI {
                                            fmt.Println("Train Num is: ", timetableInfo.TrainNum)
                                            fmt.Println("Now is: ", nowtimesec)
                                            fmt.Println("Sched Arrival is: ", timetableInfo.ArrivalSec)
                                            fmt.Println("Delay is: ", timetableInfo.Diff)
                                            fmt.Println("Angle diff is: ", timetableInfo.Diff2)
                                        }
                                    
                                        // save the train state
                                        last_state_trainnum = timetableInfo.TrainNum
                                        last_state_arrivalsec = timetableInfo.ArrivalSec
                                        last_state_actual_arrivalsec = nowtimesec
                                        last_state_delay = delaySec
                                        last_state_laststop = currentStopInfo.StationId
                                        last_state_azimuth = timetableInfo.Diff2
                                        
                                        // append to original data 
                                        JSON_data.TrainNum = strconv.Itoa(last_state_trainnum)
                                        JSON_data.DistanceTravelled = strconv.Itoa(last_state_distancetravelled)
                                        JSON_data.Delay = strconv.Itoa(last_state_delay)
                                    }else{
                                        //else, ignore it (including its pair!)
                                        if dev_imei == iWannaSeeThisIMEI {
                                            fmt.Println(dev_imei, ": OOPS!: ", train_delay[strconv.Itoa(timetableInfo.TrainNum)].imei, "ALREADY CLAIMED THIS TRAINNUM:", timetableInfo.TrainNum)

                                            //WHY?

                                            fmt.Println("BECAUSE --> YOUR DELAY IS: ", delaySec, "|", train_delay[strconv.Itoa(timetableInfo.TrainNum)].imei , "DELAY IS:", train_delay[strconv.Itoa(timetableInfo.TrainNum)].delay)
                                            fmt.Println("BECAUSE --> YOUR ANGLE DIFF IS: ", angleDiff, "|", train_delay[strconv.Itoa(timetableInfo.TrainNum)].imei , "ANGLE DIFF IS:", train_delay[strconv.Itoa(timetableInfo.TrainNum)].angle)

                                        }

                                    }

                                    

                                
                                //if we dont have any delay record yet
                                }else{
                                    

                                    // I know... maybe you can refactor this.. 
                                    if dev_imei == iWannaSeeThisIMEI {
                                        fmt.Println(dev_imei, ":GOT RECORD: ", timetableInfo.TrainNum)
                                        //panic("meh")
                                    }
                                    
                                    train_delay[strconv.Itoa(timetableInfo.TrainNum)] = train_delay_table{imei: dev_imei, delay: delaySec, angle: angleDiff}

                                    if dev_imei == iWannaSeeThisIMEI {
                                        fmt.Println("Train Num is: ", timetableInfo.TrainNum)
                                        fmt.Println("Now is: ", nowtimesec)
                                        fmt.Println("Sched Arrival is: ", timetableInfo.ArrivalSec)
                                        fmt.Println("Delay is: ", timetableInfo.Diff)
                                        fmt.Println("Angle diff is: ", timetableInfo.Diff2)
                                    }
                                
                                    // save the train state
                                    last_state_trainnum = timetableInfo.TrainNum
                                    last_state_arrivalsec = timetableInfo.ArrivalSec
                                    last_state_actual_arrivalsec = nowtimesec
                                    last_state_delay = delaySec
                                    last_state_laststop = currentStopInfo.StationId
                                    last_state_azimuth = timetableInfo.Diff2
                                    
                                    // append to original data 
                                    JSON_data.TrainNum = strconv.Itoa(last_state_trainnum)
                                    JSON_data.DistanceTravelled = strconv.Itoa(last_state_distancetravelled)
                                    JSON_data.Delay = strconv.Itoa(last_state_delay)
                                    

                                    

                                }
                                mutex.Unlock()

                                //everything OK, so we start publishing

                                //publish to Redis for JP
                                if dev_imei == iWannaSeeThisIMEI {
                                    fmt.Println("PUBLISH ================================>", dev_imei, timetableInfo.TrainNum)
                                }
                                liveQueue := fmt.Sprintf("%s%s", "L", strconv.Itoa(timetableInfo.TrainNum))
                                redisConn.Do("SET", liveQueue, delaySec)


                            }else{
                                //if we the direction is wrong we may need to look for the next trainnum available
                                //tryNextTimetable = true
                                if dev_imei == iWannaSeeThisIMEI {
                                    fmt.Println("This train is going to the other side! Let's try next trainnum", timetableInfo.TrainNum)
                                }
                                skipThisTrainnum = timetableInfo.TrainNum
                                if okToCheckAgain {
                                    okToCheckAgain = false
                                    goto CHECKTIMETABLE

                                }
                                
                            }

                        }
                        
                        
                    }
                }
            } else {
                // if both current speed and last speed is  zero, means its moving, nothing to do 
                

            }


        }

        //append original data struct
        if dev_imei == iWannaSeeThisIMEI {
            fmt.Println(dev_imei, ":PREVIOUS STATE: TRAINNUM: ", last_state_trainnum)
            fmt.Println(dev_imei, ":PREVIOUS STATE: STOP_PERIOD: ", last_idling_length)
            fmt.Println(dev_imei, ":PREVIOUS STATE: DELAY: ", last_state_delay)

            
            //panic("meh")
        }

        

        JSON_data.TrainNum = strconv.Itoa(last_state_trainnum)
        JSON_data.DistanceTravelled = strconv.Itoa(last_state_distancetravelled)
        JSON_data.Delay = strconv.Itoa(last_state_delay)
        JSON_data.IdleLength = strconv.Itoa(last_idling_length)
        JSON_data.SetNumber = imeiMap.SetNumber

        mutex.Lock()
        state[dev_imei] = state_table{trainnum: last_state_trainnum, laststop: last_state_laststop, arrivalsec: last_state_arrivalsec, actual_arrivalsec: last_state_actual_arrivalsec, delay: last_state_delay, distancetravelled: last_state_distancetravelled, azimuth: last_state_azimuth, speed: currentSpeed, idlinglength: last_idling_length, idlingtimesec: last_idling_time_sec}
        mutex.Unlock()

        // insert processed data
        
        if err := collection_pdata.Insert(JSON_data); err != nil {
            panic(err)
        }

        // only publish data to CC if we have valid data i.e., lat long is not zero 
        check,_ := strconv.ParseFloat(JSON_data.Lat, 64)
        // convert json to string
        jsonString, _ := json.Marshal(JSON_data)
        if check > 0 {
            redisConn.Do("PUBLISH", "pdata", string(jsonString))
        } 

        //insert last state to mongo
        

        id := dev_imei
        data := bson.M{"$set": bson.M{
            "trainnum": JSON_data.TrainNum,
            "speed": JSON_data.VehSpeed,
            "lat": JSON_data.Lat,
            "long": JSON_data.Long,
            "angle": JSON_data.VehAngle,
            "distance": JSON_data.DistanceTravelled,
            "delay": JSON_data.Delay,
            "laststop": currentStopInfo.StationId,
            "lastreceived": JSON_data.VehDatetimeParsed}}
        _, err := collection_laststate_data.UpsertId(id, data)
        if err != nil {
            panic(err)
        }

    }else{
        //if failed to translate, chuck the data and disconnennct this client
        fmt.Println("JSON: Invalid")
        // sessionCopy.Close()
        // redisConn.Close()
    }

}


func getStopInfo()() {



}

func translateData(dev_imei string, vehicleData string) (translated bool, dataJsonString string) {
    vehicle_data_hash := make(map[string]string)

    if len(vehicleData) == 92 {
        vehicle_data_hash["veh_status"] = vehicleData[0:8]
        vehicle_data_hash["veh_datetime"] = vehicleData[8:20]
        vehicle_data_hash["veh_batt_voltage"] = vehicleData[20:22]
        vehicle_data_hash["veh_supp_voltage"] = vehicleData[22:24]
        vehicle_data_hash["veh_ADC1"] = vehicleData[24:28]
        vehicle_data_hash["veh_temp1"] = vehicleData[28:32]
        vehicle_data_hash["veh_temp2"] = vehicleData[32:36]
        vehicle_data_hash["veh_loc"] = vehicleData[36:40]
        vehicle_data_hash["veh_cellID"] = vehicleData[40:44]
        vehicle_data_hash["veh_sat"] = vehicleData[44:46]
        vehicle_data_hash["veh_signal"] = vehicleData[46:48]
        vehicle_data_hash["veh_angle"] = vehicleData[48:51]
        vehicle_data_hash["veh_speed"] = vehicleData[51:54]
        vehicle_data_hash["veh_hdop"] = vehicleData[54:58]
        vehicle_data_hash["veh_mileage"] = vehicleData[58:65]
        vehicle_data_hash["veh_lat"] = vehicleData[65:74]
        vehicle_data_hash["veh_ns"] = vehicleData[74:75]
        vehicle_data_hash["veh_long"] = vehicleData[75:85]
        vehicle_data_hash["veh_ew"] = vehicleData[85:86]
        vehicle_data_hash["dev_IMEI"] = dev_imei
    } else if len(vehicleData) == 84 {
        vehicle_data_hash["veh_status"] = vehicleData[0:8]
        vehicle_data_hash["veh_datetime"] = vehicleData[8:20]
        vehicle_data_hash["veh_batt_voltage"] = vehicleData[20:22]
        vehicle_data_hash["veh_supp_voltage"] = vehicleData[22:24]
        vehicle_data_hash["veh_ADC1"] = vehicleData[24:28]
        vehicle_data_hash["veh_loc"] = vehicleData[28:32]
        vehicle_data_hash["veh_cellID"] = vehicleData[32:36]
        vehicle_data_hash["veh_sat"] = vehicleData[36:38]
        vehicle_data_hash["veh_signal"] = vehicleData[38:40]
        vehicle_data_hash["veh_angle"] = vehicleData[40:43]
        vehicle_data_hash["veh_speed"] = vehicleData[43:46]
        vehicle_data_hash["veh_hdop"] = vehicleData[46:50]
        vehicle_data_hash["veh_mileage"] = vehicleData[50:57]
        vehicle_data_hash["veh_lat"] = vehicleData[57:66]
        vehicle_data_hash["veh_ns"] = vehicleData[66:67]
        vehicle_data_hash["veh_long"] = vehicleData[67:78]
        vehicle_data_hash["veh_ew"] = vehicleData[78:79]
        }

    latDD := vehicle_data_hash["veh_lat"][0:2]
    latMM := vehicle_data_hash["veh_lat"][2:9]
    longDD := vehicle_data_hash["veh_long"][0:3]
    longMM := vehicle_data_hash["veh_long"][3:10]

    latDD_p,_ := strconv.ParseFloat(latDD, 64)
    latMM_p,_ := strconv.ParseFloat(latMM, 64)
    longDD_p,_ := strconv.ParseFloat(longDD, 64)
    longMM_p,_ := strconv.ParseFloat(longMM, 64)

    lat := latDD_p + (latMM_p/float64(60))
    long := longDD_p + (longMM_p/float64(60))

    vehicle_data_hash["lat"] = fmt.Sprintf("%.7f", lat)
    vehicle_data_hash["long"] = fmt.Sprintf("%.7f", long)

    
    layout := "060102150405"
    veh_datetime_parsed, _ := time.Parse(layout, vehicle_data_hash["veh_datetime"])
    // fmt.Println("TIME:",vehicle_data_hash["veh_datetime"])
    // fmt.Println("TIME:",veh_datetime_parsed)
    localtz, err := time.LoadLocation("Asia/Singapore")
    if err == nil {
        veh_datetime_parsed = veh_datetime_parsed.In(localtz)
    }
    //fmt.Println(veh_datetime_parsed.Format("01-02-2006 15:04:05"))
    vehicle_data_hash["veh_datetime_parsed"] = veh_datetime_parsed.Format("01-02-2006 15:04:05")
    datajson, _ := json.Marshal(vehicle_data_hash)
    // datajson := vehicle_data_hash
    return true, string(datajson)

}
