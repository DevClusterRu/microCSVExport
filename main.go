package main

import (
	"CidMicroservices/Utils"
	"CidMicroservices/getAllNumbers"
	"CidMicroservices/getLookupCost"
	"CidMicroservices/getLookupStat"
	"CidMicroservices/getUserPhones"
	"fmt"
	"os"
	"strings"
	"time"
)

func main()  {


	Utils.Env = make(map[string]string)
	argsArr:=strings.Split(os.Args[0],"/")
	Utils.CurrentDir = strings.Join(argsArr[:len(argsArr)-1],"/")
	fmt.Println("Current dir: "+Utils.CurrentDir)

	Utils.InitEnv()
	Utils.RedisInit()

	go func() {
		print("Wait tasks...")
		fmt.Println("I'm starting!")
		for {

			//Check pid in redis, is isset we continue iteration, another microservive working.
			//If not isset - we create record with current PID and capture driving
			//If isset BUT pid == curren pid, we update time
			//If isset, BUT current time diff more then 1 minutes - driving microservice is dead,
			//we must capture process now

			if !Utils.Alive(){
				continue
			}

			go getAllNumbers.Call()
			go getLookupCost.Call()
			go getLookupStat.Call()
			go getUserPhones.Call()

			time.Sleep(time.Second*3)
		}

	}()

	for  {
		time.Sleep(time.Second*10)
	}

}
