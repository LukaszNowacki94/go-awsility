package main

import (
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/dynamodbstreamsevt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func UnmarshalEvent(event map[string]*dynamodbstreamsevt.AttributeValue, item interface{}) error {
	return dynamodbattribute.UnmarshalMap(convert(event), item)
}

func convert(attrs map[string]*dynamodbstreamsevt.AttributeValue) map[string]*dynamodb.AttributeValue {
	parsed := make(map[string]*dynamodb.AttributeValue)
	for key, value := range attrs {
		parsed[key] = convertAttributeValue(value)
	}
	return parsed
}

func convertAttributeValue(attr *dynamodbstreamsevt.AttributeValue) *dynamodb.AttributeValue {
	switch {
	case attr.B != nil:
		return &dynamodb.AttributeValue{B: attr.B}
	case len(attr.BS) != 0:
		return &dynamodb.AttributeValue{BS: attr.BS}
	case len(attr.L) != 0:
		return &dynamodb.AttributeValue{L: convertAttributeValueList(attr.L)}
	case len(attr.M) != 0:
		return &dynamodb.AttributeValue{M: convertAttributeValueMap(attr.M)}
	case attr.N != "":
		return &dynamodb.AttributeValue{N: &attr.N}
	case len(attr.NS) != 0:
		return &dynamodb.AttributeValue{NS: convertToStringPointers(attr.NS)}
	case attr.NULL == true:
		return &dynamodb.AttributeValue{NULL: &attr.NULL}
	case attr.S != "":
		return &dynamodb.AttributeValue{S: &attr.S}
	case len(attr.SS) != 0:
		return &dynamodb.AttributeValue{SS: convertToStringPointers(attr.SS)}
	case attr.BOOL == true || attr.BOOL == false:
		return &dynamodb.AttributeValue{BOOL: &attr.BOOL}
	default:
		return nil
	}
}

func convertAttributeValueList(attrs []*dynamodbstreamsevt.AttributeValue) []*dynamodb.AttributeValue {
	newAttrs := make([]*dynamodb.AttributeValue, 0)
	for _, attr := range attrs {
		converted := convertAttributeValue(attr)
		newAttrs = append(newAttrs, converted)
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
