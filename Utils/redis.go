package Utils

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"os"
	"strconv"
	"time"
)

func RedisInit()  {
	if   Env["REDIS_PASSWORD"]=="null"{ Env["REDIS_PASSWORD"]="" }
	fmt.Println("Try redis init by login ["+Env["REDIS_HOST"]+":"+Env["REDIS_PORT"]+"] and password ["+Env["REDIS_PASSWORD"]+"] build 0.7")

	RD = redis.NewClient(&redis.Options{
		Addr:     Env["REDIS_HOST"]+":"+Env["REDIS_PORT"],
		Password: Env["REDIS_PASSWORD"], // no password set
		DB:       0,  // use default DB
	})
}

func RedisGet(key string) string {
	return RD.Get(context.Background(), key).Val()
}

func RedisDel(key string) {
	val, _ := RD.Keys(context.Background(), key).Result()
	for _,v:=range val{
		ret,_ := RD.Del(context.Background(), v).Result()
		if ret>0{
			fmt.Println(v+" key deleted")
		} else{
			fmt.Println(v+" NOT deleted!")
		}
	}
}

func RedisSet(key string, val string, exp int32) error {
	return RD.Set(context.Background(), key, val,0).Err()
}

func Alive() bool  {
	lastPingTime := RedisGet("CSVExportAlive")
	if lastPingTime != ""{
		lastPingPID := RedisGet("microCSVExportPID")
		if lastPingPID == ""{
			//Strange! Don't find pid\
			RedisSet("microCSVExportPID", strconv.Itoa(os.Getpid()),0) //Im ALIVE!
		} else{
			//Isset lastPingTime and lastPingPID

			t1 := time.Now().Unix()
			t2, _ := strconv.Atoi(lastPingTime)

			diff := t1-int64(t2)
			if diff>10{
				RedisSet("CSVExportAlive", strconv.Itoa(int(time.Now().Unix())), 0) //Im ALIVE!
				RedisSet("microCSVExportPID", strconv.Itoa(os.Getpid()),0) //Im ALIVE!
				PutLog("DIFF="+strconv.Itoa(int(diff))+"; Captured by service PID="+strconv.Itoa(os.Getpid()))
			}

			if lastPingPID != strconv.Itoa(os.Getpid()){
				//Process captured by another mservice, continue
				return false
			} else {
				//Captured by myself
				RedisSet("CSVExportAlive", strconv.Itoa(int(time.Now().Unix())),0) //Im ALIVE!
				print(".")
			}
		}
	} else {
		//Not isset CSVExportAlive, do capture
		RedisSet("CSVExportAlive", strconv.Itoa(int(time.Now().Unix())),0) //Im ALIVE!
		RedisSet("microCSVExportPID", strconv.Itoa(os.Getpid()),0) //Im ALIVE!
	}
	return true
}

//
//func csvExportAllGet() {
//	val, _ := (*RD).Keys(context.Background(), "csvExportAll:*").Result()
//	fnameRegex,_ := regexp.Compile("csvExportAll:.*")
//
//	for _,keys:=range val {
//		res := fnameRegex.FindString(keys)
//		uid:=res[13:] // This is admin ID
//
//		if !contains(CurrentUsers, uid) {
//			CurrentUsers = append(CurrentUsers, uid)
//			go rutina(uid, "csvExportAll")
//		} else {
//			fmt.Println("This user busy")
//		}
//	}
//}
//
//
//
//func csvLookupCostExport() {
//	//Format export ALL client numbers -> csvExportAll:3 : DT
//	//where 3 - id admin, DT - timestamp start operation
//
//	val, _ := RD.Keys(context.Background(), "csvLookupCostExport:*").Result()
//	fnameRegex,_ := regexp.Compile("csvLookupCostExport:.*")
//
//	for _,keys:=range val {
//		res := fnameRegex.FindString(keys)
//		uid:=res[20:] // This is admin ID
//
//		if !contains(CurrentUsers, uid) {
//			CurrentUsers = append(CurrentUsers, uid)
//			go rutina(uid, "csvLookupCostExport")
//		} else {
//			fmt.Println("This user busy")
//		}
//	}
//}
//
//func csvLookupStatExport() {
//	//Format export ALL client numbers -> csvExportAll:3 : DT
//	//where 3 - id admin, DT - timestamp start operation
//
//	val, _ := RD.Keys(context.Background(), "csvLookupStatExport:*").Result()
//	fnameRegex,_ := regexp.Compile("csvLookupStatExport:.*")
//
//	for _,keys:=range val {
//		res := fnameRegex.FindString(keys)
//		uid:=res[20:] // This is admin ID
//
//		if !contains(CurrentUsers, uid) {
//			CurrentUsers = append(CurrentUsers, uid)
//			go rutina(uid, "csvLookupStatExport")
//		} else {
//			fmt.Println("This user busy")
//		}
//	}
//}
//
//func csvLookupUserStatExport() {
//	//Format export ALL client numbers -> csvExportAll:3 : DT
//	//where 3 - id admin, DT - timestamp start operation
//
//	val, _ := RD.Keys(context.Background(), "csvLookupUserStatExport:*").Result()
//	fnameRegex,_ := regexp.Compile("csvLookupUserStatExport:.*")
//
//	for _,keys:=range val {
//		res := fnameRegex.FindString(keys)
//		uid:=res[24:] // This is admin ID
//
//		if !contains(CurrentUsers, uid) {
//			CurrentUsers = append(CurrentUsers, uid)
//			go rutina(uid, "csvLookupUserStatExport")
//		} else {
//			fmt.Println("This user busy")
//		}
//	}
//}
//
//func csvExportUserPhones() {
//	//Format export ALL numbers of one user csvExportUserPhones_TYPE
//
//	val, _ := RD.Keys(context.Background(), "csvExportUserPhones_*").Result()
//	fnameRegex,_ := regexp.Compile("_.*:")
//
//	for _,keys:=range val {
//		res := fnameRegex.FindString(keys)
//		suffix := res[1:len(res)-1]
//
//		uid:=keys[21+len(suffix):] // This is user ID
//
//		if !contains(CurrentUsers, uid) {
//			CurrentUsers = append(CurrentUsers, uid)
//			fmt.Println("rutina csvExportUserPhones_"+suffix)
//			go rutina(uid, "csvExportUserPhones_"+suffix)
//		} else {
//			fmt.Println("This user busy for csvExportUserPhones")
//		}
//	}
//}