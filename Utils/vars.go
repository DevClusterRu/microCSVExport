package Utils

import (
	"github.com/go-redis/redis/v8"
	"io/ioutil"
	"os"
	"strings"
)

var CurrentUsers []string
var Env map[string]string
var RD *redis.Client
var F *os.File
const CreatedFormat = "2006-01-02T15.04.05" //"Jan 2, 2006 at 3:04pm (MST)"
const DateFormat = "2006-01-02" //"Jan 2, 2006 at 3:04pm (MST)"
var ApiString string
var TFN []string
var CurrentDir string

func InitEnv()  {
	//file, err := os.Open("/var/www/html/calleridrep.com/.env")
	file, err := os.Open("/var/www/caller/calleridrep/src/.env")

	if err!=nil{
		panic(err)
	}
	b, _ := ioutil.ReadAll(file)
	rows:=strings.Split(string(b),"\n")
	for _,v:=range rows{
		if v==""{continue}
		pair := strings.Split(v,"=")
		if len(pair)==2{
			Env[pair[0]] = strings.TrimSpace(pair[1])
			Env[pair[0]] = strings.Trim(Env[pair[0]], "\"")
		}
	}
}