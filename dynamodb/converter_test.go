package dynamodb

import (
	"testing"
	tested "github.com/mczubak/go-awsility/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/dynamodbstreamsevt"
)

func TestConvert(t *testing.T) {
	//given
	input := make(map[string]*dynamodbstreamsevt.AttributeValue)
	list := []*dynamodbstreamsevt.AttributeValue{{N: "1"}, {N: "13"}, {N: "42"}}
	input["aString"] = &dynamodbstreamsevt.AttributeValue{S: "1"}
	input["aList"] = &dynamodbstreamsevt.AttributeValue{L: list}
	input["aBool"] = &dynamodbstreamsevt.AttributeValue{BOOL: true}
	//when
	output := tested.Convert(input)
	//then
	if len(output) != 3 {
		t.Fail()
	}
	if *output["aString"].S != input["aString"].S {
		t.Fail()
	}
	if len(output["aList"].L) != len(input["aList"].L) {
		t.Fail()
	}
	if *output["aBool"].BOOL != input["aBool"].BOOL {
		t.Fail()
	}
}

func TestUnconvert(t *testing.T) {
	//given
	input := make(map[string]*dynamodb.AttributeValue)
	list := []*dynamodb.AttributeValue{{N: stringPtr("1")}, {N: stringPtr("13")}, {N: stringPtr("42")}}
	input["aString"] = &dynamodb.AttributeValue{S: stringPtr("stronk")}
	input["aList"] = &dynamodb.AttributeValue{L: list}
	input["aBool"] = &dynamodb.AttributeValue{BOOL: boolPtr(true)}
	//when
	output := tested.Unconvert(input)
	//then
	if len(output) != 3 {
		t.Fail()
	}
	if output["aString"].S != *input["aString"].S {
		t.Fail()
	}
	if len(output["aList"].L) != len(input["aList"].L) {
		t.Fail()
	}
	if output["aBool"].BOOL != *input["aBool"].BOOL {
		t.Fail()
	}
}

func stringPtr(s string) *string{
	return &s
}

func boolPtr(s bool) *bool{
	return &s
}
