package main

import (
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/aws"
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"log"
	"github.com/mczubak/go-awsility/cloudwatch"
)

func main() {
	s, _ := session.NewSession(aws.NewConfig().WithRegion("eu-west-1"))
	cl := &cloudwatch.Client{Cloudwatch: cw.New(s)}
	res, _ := cl.GetLastNMinMetrics(300, "AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName", "OTA-TEST", 60)
	for _, i := range res {
		log.Println(i)
	}
	//r, _ := cl.getAlarmByPrefix("FR-OTA-OPR-AVAILABILITIES-WRITE-SCALE-DOWN")
	//fmt.Println(r)
}