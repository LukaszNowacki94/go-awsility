package dynamodb

import (
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/dynamodbstreamsevt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func UnmarshalEvent(event map[string]*dynamodbstreamsevt.AttributeValue, item interface{}) {
	dynamodbattribute.UnmarshalMap(convert(event), item)
}

func convert(attrs map[string]*dynamodbstreamsevt.AttributeValue) map[string]*dynamodb.AttributeValue {
	parsed := make(map[string]*dynamodb.AttributeValue)
	for key, value := range attrs {
		parsed[key] = convertAttributeValue(value)
	}
	return parsed
}

func convertAttributeValue(attr *dynamodbstreamsevt.AttributeValue) *dynamodb.AttributeValue {
	return &dynamodb.AttributeValue{B: attr.B,
		BOOL: &attr.BOOL,
		BS: attr.BS,
		L: convertAttributeValueList(attr.L),
		M: convertAttributeValueMap(attr.M),
		N: &attr.N,
		NS: convertToStringPointers(attr.NS),
		NULL: &attr.NULL,
		S: &attr.S,
		SS: convertToStringPointers(attr.SS)}
}

func convertAttributeValueList(attrs []*dynamodbstreamsevt.AttributeValue) []*dynamodb.AttributeValue {
	newAttrs := make([]*dynamodb.AttributeValue, len(attrs))
	for _, attr := range attrs {
		newAttrs = append(newAttrs, convertAttributeValue(attr))
	}
	return newAttrs
}

func convertAttributeValueMap(attrs map[string]*dynamodbstreamsevt.AttributeValue) map[string]*dynamodb.AttributeValue {
	newAttrs := make(map[string]*dynamodb.AttributeValue)
	for key, value := range attrs {
		newAttrs[key] = convertAttributeValue(value)
	}
	return newAttrs
}

func convertToStringPointers(strings []string) []*string {
	newStrings := make([]*string, len(strings))
	for _, str := range strings {
		newStrings = append(newStrings, &str)
	}
	return newStrings
}
