package Utils

import (
	"database/sql"
	"fmt"
	"os"
	_ "github.com/go-sql-driver/mysql"
)

func Connection() *sql.DB {
	dbString := fmt.Sprintf("%v:%v@(%v)/%v", Env["DB_USERNAME"], Env["DB_PASSWORD"], Env["DB_HOST"], Env["DB_DATABASE"])
	fmt.Println("Forming MYSQL string: "+dbString)

	db, err := sql.Open("mysql", dbString)
	if err != nil {
		ErrLogger(err, "Error when DBInit","")
		os.Exit(400)
	}
	return db
}

func LookupHeadersGet(redisKey string, Env map[string]string) *[]string {
	//Get headers
	rows, err := Connection().Query("SELECT `alias` FROM lookups_cost WHERE `alias` IS NOT NULL")
	fmt.Println(err, "Error when lookups_cost headers",redisKey)
	defer rows.Close()
	var header string
	var headers []string

	for rows.Next() {
		err := rows.Scan(&header)
		headers = append(headers, header)

		ErrLogger(err, "Error when parse lookups_cost headers",redisKey)
	}

	return &headers
}

func UserApiKey(uid string) string  {
	var key string
	s:=`SELECT api_token FROM users WHERE id = ?;`;
	row := Connection().QueryRow(s, uid)
	switch err := row.Scan(&key); err {
	case sql.ErrNoRows:
		return "NoRows"
	case nil:
		return "OK"+key
	default:
		return err.Error()
	}
}

func Fwrite(f *os.File, s string)  {
	f.WriteString(s)
	f.WriteString(",")
}


