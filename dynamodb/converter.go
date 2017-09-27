package dynamodb

import (
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/dynamodbstreamsevt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/guregu/dynamo"
)

func UnmarshalEvent(event map[string]*dynamodbstreamsevt.AttributeValue, item interface{}) error {
	return dynamo.UnmarshalItem(convert(event), item)
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
	case attr.BOOL == true || attr.BOOL == false:
		return &dynamodb.AttributeValue{BOOL: &attr.BOOL}
	case attr.BS != nil:
		return &dynamodb.AttributeValue{BS: attr.BS}
	case attr.L != nil:
		return &dynamodb.AttributeValue{L: convertAttributeValueList(attr.L)}
	case attr.M != nil:
		return &dynamodb.AttributeValue{M: convertAttributeValueMap(attr.M)}
	case attr.N != "":
		return &dynamodb.AttributeValue{N: &attr.N}
	case attr.NS != nil:
		return &dynamodb.AttributeValue{NS: convertToStringPointers(attr.NS)}
	case attr.NULL:
		return &dynamodb.AttributeValue{NULL: &attr.NULL}
	case attr.S != "":
		return &dynamodb.AttributeValue{S: &attr.S}
	case attr.SS != nil:
		return &dynamodb.AttributeValue{SS: convertToStringPointers(attr.SS)}
	}
	return nil
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
