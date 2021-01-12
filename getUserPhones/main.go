package getUserPhones

import (
	"CidMicroservices/Utils"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"
)

type CurrentViewParams struct {
	Archive   string
	Number    string
	Sort      string
	Direction string
	Managed   string
	Start     string
	End       string
	Period    string
	Code      string
	Area      string
}

var headers = []string{
"CLIENT_ID", "MANAGER_ID", "CLIENT_NAME", "CLIENT_EMAIL",
"Phone", "CNAM", "Description", "TYPE",
"DateAdded", "DateFlagged", "Archive", "Managed", "Call_Group",
"Carrier_Service_Provider", "State"}

type number struct {
	created_at                  string
	date_first_flagged          string
	user_id                     string
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

//var params CurrentViewParams
var params CurrentViewParams
var flags  = [13]string{
	"ftc_flagged",
	"nomorobo_flagged",
	"robokiller_flagged",
	"whitepages_flagged",
	"callcontrol_flagged",
	"hiya_flagged",
	"mcc_flagged",
	"ihs_flagged",
	"twhite_flagged",
	"tnomo_flagged",
	"tts_flagged",
	"whitepages_level",
	"twhite_level",
}


func Call()  {


	val, _ := Utils.RD.Keys(context.Background(), "csvExportUserPhones_*").Result()
	fnameRegex,_ := regexp.Compile("_.*:")

	for _,keys:=range val {
		res := fnameRegex.FindString(keys)
		suffix := res[1:len(res)-1]

		uid:=keys[21+len(suffix):] // This is user ID

		if !Utils.Contains(Utils.CurrentUsers, uid) {
			Utils.CurrentUsers = append(Utils.CurrentUsers, uid)
			fmt.Println("rutina csvExportUserPhones_"+suffix)
			Doit(uid, suffix)
		} else {
			fmt.Println("This user busy for csvExportUserPhones")
		}
	}


}

func Doit(uid string, exType string)  {

	fname := Utils.CurrentDir + "/userPhones" + time.Now().Format(Utils.CreatedFormat)

	flaggedStr := ""
	unflaggedStr := ""
	for _, v := range flags {
		flaggedStr += "t5." + v + " IS NOT NULL OR "
		unflaggedStr += "t5." + v + " IS NULL AND "
	}
	if flaggedStr != "" {
		flaggedStr = flaggedStr[0 : len(flaggedStr)-4]
		unflaggedStr = unflaggedStr[0 : len(unflaggedStr)-3]
	}

	where := "WHERE t1.deleted_at IS NULL AND t2.id = " + uid + " "
	switch exType {
	case "flagged":
		if flaggedStr != "" {
			where += `AND (` + flaggedStr + `) `
		} else {
			where += ""
		}
	case "unflagged":
		if unflaggedStr != "" {
			where += `AND (` + unflaggedStr + `) `
		} else {
			where += ""
		}
	case "exportCurrentView":
		parms := Utils.RedisGet("csvExportUserPhonesParam:" + uid)
		if len(parms) == 0 {
			Utils.PutLog("Strange, but params is't isset, canceling...")
			Utils.RedisDel("csvExportUserPhones_exportCurrentView:" + uid)
			Utils.CurrentUsers = Utils.Pop(Utils.CurrentUsers, uid)
			return
		}
		err := json.Unmarshal([]byte(parms), &params)
		Utils.ErrLogger(err, "when unmarshal params","csvExportUserPhones_"+exType+":"+uid)

		if params.Number != "" {
			where += " AND t1.number LIKE '%" + params.Number + "%' "
		}

		if params.Archive == "YES" {
			where += " AND t1.archive IS NOT NULL "
		}
		if params.Managed != "" {
			where += " AND t2.manager_id IS NOT NULL "
		}
		if params.Start != "" {
			where += " AND date(t1.created_at) >= '" + params.Start + "' "
		}
		if params.End != "" {
			where += " AND date(t1.created_at) <= '" + params.End + "' "
		}

		if params.Code != "" {
			where += " AND t1.state_code = '" + params.Code + "' "
		}
		//{"number":"123","sort":"created_at","direction":"desc"}
		if params.Sort != "" {
			where += " ORDER BY t1." + params.Sort
		}
		if params.Direction != "" {
			where += " " + params.Direction
		}

	case "usages":
		usagesFrom := Utils.RedisGet("csvExportUserPhonesUsagesFrom:" + uid)
		if len(usagesFrom) == 0 {
			Utils.PutLog("Strange, but params is't isset, canceling...")
			Utils.RedisDel("csvExportUserPhones_usages:" + uid)
			Utils.CurrentUsers = Utils.Pop(Utils.CurrentUsers, uid)
			return
		}
		where += " AND t6.added_at >= '" + usagesFrom + "' "

	}
	//&amp;number=134&amp;sort=flagged.date_first_flagged&amp;direction=desc

	query := `
	SELECT 
		t1.created_at,
		t1.user_id,
		`
	if flaggedStr != "" {
		query += `IF ( (` + flaggedStr + `), "-", t5.date_first_flagged),`
	} else {
		query += `"-", `
	}
	query +=
		`
		IF(t2.manager_id IS NULL, "-", t2.manager_id),
		t2.name,
		t2.email,
		t1.number,
		IF(t1.cnam IS NULL, "-", t1.cnam),
		IF(t1.description IS NULL, "-", t1.description),
		IF(t1.archive IS NULL, "-", t1.archive),
		IF(t1.carrier_service_provider_id IS NULL, "", t1.carrier_service_provider_id),
		IF(t3.name IS NULL, "", t3.name) AS groupName,
		IF(t4.data IS NULL, "-", t4.data),
		IF(t1.state_code IS NULL, "-", t1.state_code)
		FROM phones AS t1
		LEFT JOIN users AS t2 ON t1.user_id = t2.id
		LEFT JOIN call_groups AS t3 ON t1.call_group_id = t3.id
		LEFT JOIN carrier_service_providers AS t4 ON t1.carrier_service_provider_id = t4.id
		LEFT JOIN phones_usages AS t6 ON t1.id = t6.phone_id
		LEFT JOIN phones_flagged AS t5 ON t1.id = t5.phone_id ` + where

	rows, err := Utils.Connection().Query(query)

	Utils.ErrLogger(err, "Error when allNumbersSequence","csvExportUserPhones_"+exType+":"+uid)
	defer rows.Close()

	file, err := os.Create(fname + ".csv")
	if err != nil {
		Utils.ErrLogger(err, "Unable to create file","csvExportUserPhones_"+exType+":"+uid)
		return
	}
	defer file.Close()

	head := ""
	for k, v := range headers {
		head += v
		if k < len(headers)-1 {
			head += ","
		} else {
			head += "\n"
		}
	}

	file.WriteString(head)

	for rows.Next() {
		n := number{}
		p := carrier_service_provider{}
		err := rows.Scan(&n.created_at, &n.date_first_flagged, &n.user_id, &n.manager_id,
			&n.name, &n.email, &n.number, &n.cnam, &n.description, &n.archive,
			&n.carrier_service_provider_id, &n.groupName, &n.data, &n.state_code)
		if err != nil {
			Utils.ErrLogger(err, "Error when parse allNumbersSequence","csvExportUserPhones_"+exType+":"+uid)
			continue
		}
		if len(n.data) > 1 {
			err = json.Unmarshal([]byte(n.data), &p)
			Utils.ErrLogger(err, "Error when unmarshall allNumbersSequence","csvExportUserPhones_"+exType+":"+uid)
			n.data = p.FriendlyName
		}

		//"CLIENT_ID", "MANAGER_ID", "CLIENT_NAME", "CLIENT_EMAIL",
		//"Phone", "CNAM", "Description", "TYPE",
		//"DateAdded", "DateFlagged", "Archive", "Managed", "Call_Group",
		//"Carrier_Service_Provider", "State"
		Utils.Fwrite(file, n.user_id)
		Utils.Fwrite(file, n.manager_id)
		Utils.Fwrite(file, n.name)
		Utils.Fwrite(file, n.email)
		Utils.Fwrite(file, n.number)
		Utils.Fwrite(file, n.cnam)
		Utils.Fwrite(file, n.description)

		startPos := 0
		if len(n.number) == 11 {
			startPos = 1
		}
		nType := "LOCAL"
		if Utils.Contains(Utils.TFN, n.number[startPos:3]) {
			nType = "TFN"
		}
		Utils.Fwrite(file, nType)
		Utils.Fwrite(file, n.created_at)
		Utils.Fwrite(file, n.date_first_flagged)
		Utils.Fwrite(file, n.archive)

		managed := "N"
		if n.carrier_service_provider_id != "" {
			managed = "Y"
		}

		Utils.Fwrite(file, managed)
		Utils.Fwrite(file, n.groupName)
		Utils.Fwrite(file, n.data)
		file.WriteString(n.state_code)
		file.WriteString("\n")
		//"Carrier_Service_Provider", "State")
	}

	Utils.PutLog("Try del redis key csvExportUserPhonesParam:" + uid)
	Utils.RedisDel("csvExportUserPhonesParam:" + uid)
	Utils.PutLog("Try del redis key csvExportAllFields:" + uid)
	Utils.RedisDel("csvExportAllFields:" + uid)

	Utils.PrepareAndSend(uid, fname, "csvExportUserPhones_"+exType, "Phones collection file")
	Utils.PutLog("DONE")

}