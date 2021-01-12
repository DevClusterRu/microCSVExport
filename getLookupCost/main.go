package getLookupCost

import (
	"CidMicroservices/Utils"
	"context"
	"fmt"
	"os"
	"regexp"
	"time"
)

type lookupsCost struct{
	User string
	Date string
	Service string
	Cost float32
}


var headers = []string{
	"CLIENT_ID", "MANAGER_ID", "CLIENT_NAME", "CLIENT_EMAIL",
	"Phone", "CNAM", "Description", "TYPE",
	"DateAdded", "DateFlagged", "Archive", "Managed", "Call_Group",
	"Carrier_Service_Provider", "State"}

type number struct {
	created_at                  string
	user_id                     string
	date_first_flagged          string
	manager_id                  string
	name                        string
	email                       string
	number                      string
	cnam                        string
	description                 string
	archive                     string
	carrier_service_provider_id string
	groupName                   string
	data                        string
	state_code                  string
}

type carrier_service_provider struct {
	FriendlyName string
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


	val, _ := Utils.RD.Keys(context.Background(), "csvLookupCostExport:*").Result()
	fnameRegex,_ := regexp.Compile("csvLookupCostExport:.*")

	for _,keys:=range val {
		res := fnameRegex.FindString(keys)
		uid:=res[20:] // This is admin ID

		if !Utils.Contains(Utils.CurrentUsers, uid) {
			Utils.CurrentUsers = append(Utils.CurrentUsers, uid)
			Doit(uid)
		} else {
			fmt.Println("This user busy")
		}
	}

}

func Doit(uid string)  {
	headers:=lookupHeadersGet("csvLookupCostExport:"+uid)

	//Get info
	rows, err := Utils.Connection().Query(`
				SELECT tmp.user_id, date(tmp.created_at), tmp.service, (sum(tmp.cc) * tmp.cost) as cc
				FROM
                (
					(
					SELECT 
						user_id,
						api_calls.created_at,
						api_calls.service,
						count(*) as cc,
						lookups_cost.cost
					FROM api_calls
					LEFT JOIN lookups_cost ON lookups_cost.alias = api_calls.service
					WHERE lookups_cost.cost IS NOT NULL #AND user_id = 77 AND date(api_calls.created_at) = '2020-11-08'
					GROUP BY date(api_calls.created_at), user_id, api_calls.service
					)	 
						UNION
					(
					SELECT 
						user_id,
						api_calls_summary.date,
						api_calls_summary.service,
						api_calls_summary.count as cc,						
						lookups_cost.cost    
					FROM api_calls_summary
					LEFT JOIN lookups_cost ON lookups_cost.alias = api_calls_summary.service
					WHERE api_calls_summary.user_id IS NOT NULL #AND user_id = 77 AND date = '2020-11-08'
					AND api_calls_summary.user_id <> 0
					 
					) 
				) as tmp				               
				WHERE tmp.cost is not null
				GROUP BY user_id, date(created_at), service
			
	`)
	Utils.ErrLogger(err, "Error when get info for lookups_cost","csvLookupCostExport:"+uid)
	defer rows.Close()

	fname:=Utils.CurrentDir+"/lookupCost"+time.Now().Format(Utils.CreatedFormat)

	file, err := os.Create(fname+".csv")
	if err != nil{
		Utils.ErrLogger(err, "Unable to create file","csvLookupCostExport:"+uid)
		return
	}

	head:="USER_ID,DATE,"
	for k, v := range headers {
		head+=v
		if k<len(headers)-1{head+=","}else{head+="\n"}
	}
	file.WriteString(head)

	cs := make (map[string]map[string]float32) //Result collection map keys - user_date, second map - array of service -> price

	var cost lookupsCost
	//c:=0

	for rows.Next() {
		err := rows.Scan(&cost.User, &cost.Date, &cost.Service, &cost.Cost)
		_, ok := cs[cost.User+","+cost.Date]
		if !ok {
			cs[cost.User+","+cost.Date] = make (map[string]float32)
		}
		cs[cost.User+","+cost.Date][cost.Service] =  cost.Cost
		Utils.ErrLogger(err, "Error when parse lookups_cost collect","csvLookupCostExport:"+uid)
	}

	for k,val:=range cs{
		csvRow:= k //k = 77,2020-11-08
		for _,header:=range headers{

			if val[header]==0{
				csvRow+=",0.00$"
			} else {
				csvRow+="," + fmt.Sprintf("%v", val[header])
			}
		}
		file.WriteString(csvRow+"\n")
	}
	defer file.Close()

	Utils.PrepareAndSend(uid, fname, "csvLookupCostExport","Csv numbers collection")
	Utils.PutLog("DONE")

}