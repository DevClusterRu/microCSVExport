package getAllNumbers

import (
	"CidMicroservices/Utils"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"time"
)

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

func Call()  {

	val, _ := Utils.RD.Keys(context.Background(), "csvExportAll:*").Result()
		fnameRegex,_ := regexp.Compile("csvExportAll:.*")

		for _,keys:=range val {
			res := fnameRegex.FindString(keys)
			uid:=res[13:] // This is admin ID

			if !Utils.Contains(Utils.CurrentUsers, uid) {
				Utils.CurrentUsers = append(Utils.CurrentUsers, uid)
				Doit(uid)
			} else {
				fmt.Println("This user busy")
			}
		}

}

func Doit(uid string)  {

	q := `
	SELECT 
		t1.created_at AS CREATED_AT,
		t1.user_id AS USER_ID,
		IF (t5.date_first_flagged IS NULL, "-", t5.date_first_flagged) AS MANAGER_ID,
		IF (t2.manager_id IS NULL, "-", t2.manager_id) AS MANAGER_ID,
		t2.name AS NAME,
		t2.email AS EMAIL,
		t1.number  AS NUMBER,
		IF(t1.cnam IS NULL, "-", t1.cnam) AS CNAME,
		IF(t1.description IS NULL, "-", t1.description) AS DESCRIPTION,
		IF(t1.archive IS NULL, "-", t1.archive) AS ARCHIVE,
		IF(t1.carrier_service_provider_id IS NULL, "", t1.carrier_service_provider_id) AS CSP_ID,
		IF(t3.name IS NULL, "", t3.name) AS GROUP_NAME,
		IF(t4.data IS NULL, "-", t4.data) AS CSP_DATA,
		IF(t1.state_code IS NULL, "-", t1.state_code) AS STATE_CODE
	FROM phones AS t1
	LEFT JOIN users AS t2 ON t1.user_id = t2.id
	LEFT JOIN call_groups AS t3 ON t1.call_group_id = t3.id
	LEFT JOIN carrier_service_providers AS t4 ON t1.carrier_service_provider_id = t4.id
	LEFT JOIN phones_flagged AS t5 ON t1.id = t5.phone_id WHERE t1.deleted_at IS NULL`

	rows, err := Utils.Connection().Query(q)

	Utils.ErrLogger(err, "Error when allNumbersSequence","csvExportAll:"+uid)
	defer rows.Close()

	fname := Utils.CurrentDir+"/allNumbers"+time.Now().Format(Utils.CreatedFormat)

	file, err := os.Create(fname + ".csv")
	if err != nil {
		Utils.ErrLogger(err, "Unable to create file","csvExportAll:"+uid)
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
		err := rows.Scan(&n.created_at, &n.user_id, &n.date_first_flagged, &n.manager_id,
			&n.name, &n.email, &n.number, &n.cnam, &n.description, &n.archive,
			&n.carrier_service_provider_id, &n.groupName, &n.data, &n.state_code)
		if err != nil {
			Utils.ErrLogger(err, "Error when parse allNumbersSequence","csvExportAll:"+uid)
			continue
		}
		if len(n.data) > 1 {
			err = json.Unmarshal([]byte(n.data), &p)
			Utils.ErrLogger(err, "Error when unmarshall allNumbersSequence","csvExportAll:"+uid)
			n.data = p.FriendlyName
		}

		//CLIENT_ID", "MANAGER_ID", "CLIENT_NAME", "CLIENT_EMAIL"
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
		if n.manager_id != "" {
			managed = "Y"
		}
		Utils.Fwrite(file, managed)
		Utils.Fwrite(file, n.groupName)
		Utils.Fwrite(file, n.data)
		file.WriteString(n.state_code)
		file.WriteString("\n")
		//"Carrier_Service_Provider", "State")
	}

	Utils.PrepareAndSend(uid, fname, "csvExportAll", "All numbers collection")
	Utils.PutLog("DONE")
}