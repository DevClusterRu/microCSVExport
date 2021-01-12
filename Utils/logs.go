package Utils

import (
	"fmt"
	"strings"
)




func ErrLogger(err error, txt string, redisKeyForDel string)  {
	if err != nil {
		PutLog(err.Error())
		PutLog(txt)
		RedisDel(redisKeyForDel)
		CurrentUsers = Pop(CurrentUsers,redisKeyForDel[strings.Index(redisKeyForDel,":")+1:])
	}
}

func PutLog(msg string)  {
	fmt.Println(msg)
}