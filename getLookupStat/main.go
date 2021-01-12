package getLookupStat

import (
	"CidMicroservices/Utils"
	"context"
	"fmt"
	"os"
	"regexp"
	"time"
)

type lookupsStat struct{
	User string
	Date string
	Service string
	Count int
}

func lookupHeadersGet(redisKey string) []string {
	//Get headers
	rows, err := Utils.Connection().Query("SELECT `alias` FROM lookups_cost WHERE `alias` IS NOT NULL")
	Utils.ErrLogger(err, "Error when lookups_cost headers",redisKey)
	defer rows.Close()
	var header string
	var headers []string
	for rows.Next() {
		err := rows.Scan(&header)
		headers = append(headers, header)
		Utils.ErrLogger(err, "Error when parse lookups_cost headers",redisKey)
	}
	return headers
}

func Call()  {


	val, _ := Utils.RD.Keys(context.Background(), "csvLookupStatExport:*").Result()
	fnameRegex,_ := regexp.Compile("csvLookupStatExport:.*")

	for _,keys:=range val {
		res := fnameRegex.FindString(keys)
		uid:=res[20:] // This is admin ID

		if !Utils.Contains(Utils.CurrentUsers, uid) {
			Utils.CurrentUsers = append(Utils.CurrentUsers, uid)
			Doit(uid, "admin")
		} else {
			fmt.Println("This user busy")
		}
	}


	val, _ = Utils.RD.Keys(context.Background(), "csvLookupUserStatExport:*").Result()
	fnameRegex,_ = regexp.Compile("csvLookupUserStatExport:.*")

	for _,keys:=range val {
		res := fnameRegex.FindString(keys)
		uid:=res[24:] // This is admin ID

		if !Utils.Contains(Utils.CurrentUsers, uid) {
			Utils.CurrentUsers = append(Utils.CurrentUsers, uid)
			Doit(uid, "client")
		} else {
			fmt.Println("This user busy")
		}
	}

}

func Doit(uid string, userType string)  {

	usr:=""
	redisKey:=""
	if userType=="admin"{
		usr=" IS NOT NULL "
		redisKey = "csvLookupStatExport"
	} else {
		usr=" =  "+uid+" "
		redisKey = "csvLookupUserStatExport"
	}
	headers:=lookupHeadersGet(redisKey+":"+uid)

	//Get info
	rows, err := Utils.Connection().Query(`
		SELECT tmp.user_id, date(tmp.created_at), tmp.service, sum(cc)
				FROM
                (
					(
						SELECT 
							api_calls.user_id,
							api_calls.created_at,
							api_calls.service,
							count(*) as cc
						FROM api_calls
                        WHERE api_calls.user_id IS NOT NULL
						GROUP BY date(api_calls.created_at), api_calls.service
					)	 
						UNION
					(
					SELECT 
							api_calls_summary.user_id,
							api_calls_summary.date,
							api_calls_summary.service,
							api_calls_summary.count as cc						
						FROM api_calls_summary
                        WHERE api_calls_summary.user_id IS NOT NULL
					) 
				) as tmp				               
				WHERE tmp.user_id `+usr+`
				GROUP BY tmp.user_id, date(tmp.created_at), tmp.service
                ORDER BY created_at DESC
	`)
	Utils.ErrLogger(err, "Error when get info for lookups_stat",redisKey+":"+uid)
	defer rows.Close()

	fname:=Utils.CurrentDir+"/lookupStat"+time.Now().Format(Utils.CreatedFormat)

	file, err := os.Create(fname+".csv")
	if err != nil{
		Utils.ErrLogger(err, "Unable to create file",redisKey+":"+uid)
		return
	}

	head:="UID,DATE,"
	for k, v := range headers {
		head+=v
		if k<len(headers)-1{head+=","}else{head+="\n"}
	}

	file.WriteString(head)

	cs := make (map[string]map[string]int) //Result collection map keys - user_date, second map - array of service -> price

	var cost lookupsStat
	//c:=0

	var datesDesc []string

	for rows.Next() {
		err := rows.Scan(&cost.User, &cost.Date, &cost.Service, &cost.Count)
		if !Utils.Contains(datesDesc, cost.User+","+cost.Date) {
			datesDesc = append(datesDesc, cost.User+","+cost.Date)
		}
		//TODO Stop here. We get structure like '4', '2020-10-03 00:00:00', 'FTC', '222.6'
		_, ok := cs[cost.User+","+cost.Date]
		if !ok {
			cs[cost.User+","+cost.Date] = make (map[string]int)
		}
		cs[cost.User+","+cost.Date][cost.Service] =  cost.Count
		Utils.ErrLogger(err, "Error when parse lookups_stat collect",redisKey+":"+uid)
	}

	for _,dateDSC:=range datesDesc{ //val =  map[FTC:5 ICEHOOK:6 NOMOROBO:7]
		val := cs[dateDSC]
		csvRow:= dateDSC //k = 77,2020-11-08

		for _,header:=range headers{

			if val[header]==0{
				csvRow += ",0"
			} else {
				csvRow+="," + fmt.Sprintf("%v", val[header])
			}

		}
		//fmt.Println(csvRow+"\n")
		file.WriteString(csvRow+"\n")
	}
	defer file.Close()

	//TODO Dont forget change all redis keys!

	Utils.PrepareAndSend(uid, fname, redisKey,"Lookups stat file")
	Utils.PutLog("DONE")

}
