package Utils

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
)

func toS3(filename string) minio.UploadInfo {
	ctx := context.Background()

	endpoint := "s3.amazonaws.com"
	accessKeyID := Env["AWS_ACCESS_KEY_ID"]
	secretAccessKey := Env["AWS_SECRET_ACCESS_KEY"]
	useSSL := true

	fmt.Println(accessKeyID+"; "+secretAccessKey)

	// Initialize minio client object.
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKeyID, secretAccessKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		log.Fatalln(err)
	}

	// Make a new bucket called CSVExport.
	bucketName := Env["AWS_BUCKET"]

	// Upload the zip file
	objectName := "microCSV/"+filepath.Base(filename)
	filePath := filename
	contentType := "application/zip"

	// Upload the zip file with FPutObject
	n, err := minioClient.FPutObject(ctx, bucketName, objectName, filePath, minio.PutObjectOptions{ContentType: contentType})
	if err != nil {
		log.Fatalln(err)
	}

	return n

}

func appendFiles(filename string, zipw *zip.Writer) error {
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("Failed to open %s: %s", filename, err)
	}
	defer file.Close()

	wr, err := zipw.Create(filepath.Base(filename))
	if err != nil {
		msg := "Failed to create entry for %s in zip file: %s"
		return fmt.Errorf(msg, filename, err)
	}

	if _, err := io.Copy(wr, file); err != nil {
		return fmt.Errorf("Failed to write %s to zip: %s", filename, err)
	}

	return nil
}

func zipper(fname string, output string)  {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	file, err := os.OpenFile(output, flags, 0644)
	if err != nil {
		log.Fatalf("Failed to open zip for writing: %s", err)
	}
	defer file.Close()

	zipw := zip.NewWriter(file)
	defer zipw.Close()

	if err := appendFiles(fname, zipw); err != nil {
		log.Fatalf("Failed to add file %s to zip: %s", fname, err)
	}
}

func api(command string, redisKey string)  {

	fmt.Println(ApiString+command)

	resp, err := http.Get(ApiString+command)
	if err!=nil {
		ErrLogger(err, "Error when send via api",redisKey)
	} else {
		var rsp []byte
		rsp, _ = ioutil.ReadAll(resp.Body)
		PutLog("Send notify to CID ("+command+") and response: " + string(rsp))
	}
}

func PrepareAndSend(receiverUid string, fname string, redisKey string, title string)  {

	apikey:= UserApiKey(receiverUid)
	if apikey[0:2] != "OK"{
		PutLog("Something wrong when api key get "+apikey)
		return
	}

	//adminMail:=RedisGet(redisKey+":"+receiverUid)
	zipper(fname+".csv", fname+".zip")
	fmt.Println("ZIP prepared")
	uploadInfo := toS3(fname+".zip")
	fmt.Println("ZIP sended, link: ", "https://s3.amazonaws.com/"+uploadInfo.Bucket+"/"+uploadInfo.Key)

	//fmt.Println(userApiKey("1"))
	ApiString = Env["APP_URL"]+"/api/v1/sendNotify?api_token="+apikey[2:]

	fmt.Println("===>",ApiString)

	api("&uid="+receiverUid+"&title="+url.QueryEscape(title)+"&link="+url.QueryEscape("https://s3.amazonaws.com/"+uploadInfo.Bucket+"/"+uploadInfo.Key),redisKey+":"+receiverUid)

	err := os.Remove(fname+".csv")
	ErrLogger(err,"Wrong remove "+fname+".csv",redisKey+":"+receiverUid)
	err = os.Remove(fname+".zip")
	ErrLogger(err,"Wrong remove "+fname+".zip",redisKey+":"+receiverUid)
	PutLog("Try del redis key "+redisKey+":"+receiverUid)
	RedisDel(redisKey+":"+receiverUid)
	CurrentUsers = Pop(CurrentUsers,receiverUid)
}
