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
	awsClient := cw.New(s)
	metricAlarm := cloudwatch.Client{Cloudwatch: awsClient}
	sum, err := metricAlarm.GetLast5MinMetrics("AWS/DynamoDB", "ConsumedWriteCapacityUnits", "TableName",
		"FR-OTA-OPR-AVAILABILITIES-PROD", 60)
	if err != nil {
		log.Println(err)
	}
	log.Println(sum)
}
