package dynamodb

import (
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/dynamodbstreamsevt"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func UnmarshalEvent(event map[string]*dynamodbstreamsevt.AttributeValue, item interface{}) error {
	return dynamodbattribute.UnmarshalMap(Convert(event), item)
}

func Convert(attrs map[string]*dynamodbstreamsevt.AttributeValue) map[string]*dynamodb.AttributeValue {
	parsed := make(map[string]*dynamodb.AttributeValue)
	for key, value := range attrs {
		parsed[key] = convertAttributeValue(value)
	}
	return parsed
}

func Unconvert(attrs map[string]*dynamodb.AttributeValue) map[string]*dynamodbstreamsevt.AttributeValue {
	parsed := make(map[string]*dynamodbstreamsevt.AttributeValue)
	for key, value := range attrs {
		parsed[key] = unconvertAttributeValue(value)
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

func unconvertAttributeValue(attr *dynamodb.AttributeValue) *dynamodbstreamsevt.AttributeValue {
	switch {
	case attr.B != nil:
		return &dynamodbstreamsevt.AttributeValue{B: attr.B}
	case len(attr.BS) != 0:
		return &dynamodbstreamsevt.AttributeValue{BS: attr.BS}
	case len(attr.L) != 0:
		return &dynamodbstreamsevt.AttributeValue{L: unconvertAttributeValueList(attr.L)}
	case len(attr.M) != 0:
		return &dynamodbstreamsevt.AttributeValue{M: unconvertAttributeValueMap(attr.M)}
	case attr.N != nil:
		return &dynamodbstreamsevt.AttributeValue{N: *attr.N}
	case len(attr.NS) != 0:
		return &dynamodbstreamsevt.AttributeValue{NS: unconvertToStringPointers(attr.NS)}
	case attr.NULL != nil:
		return &dynamodbstreamsevt.AttributeValue{NULL: *attr.NULL}
	case attr.S != nil:
		return &dynamodbstreamsevt.AttributeValue{S: *attr.S}
	case len(attr.SS) != 0:
		return &dynamodbstreamsevt.AttributeValue{SS: unconvertToStringPointers(attr.SS)}
	case attr.BOOL != nil:
		return &dynamodbstreamsevt.AttributeValue{BOOL: *attr.BOOL}
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

func unconvertAttributeValueList(attrs []*dynamodb.AttributeValue) []*dynamodbstreamsevt.AttributeValue {
	newAttrs := make([]*dynamodbstreamsevt.AttributeValue, 0)
	for _, attr := range attrs {
		converted := unconvertAttributeValue(attr)
		newAttrs = append(newAttrs, converted)
	}
	return newAttrs
}

func unconvertAttributeValueMap(attrs map[string]*dynamodb.AttributeValue) map[string]*dynamodbstreamsevt.AttributeValue {
	newAttrs := make(map[string]*dynamodbstreamsevt.AttributeValue)
	for key, value := range attrs {
		newAttrs[key] = unconvertAttributeValue(value)
	}
	return newAttrs
}

func unconvertToStringPointers(strings []*string) []string {
	newStrings := make([]string, len(strings))
	for _, str := range strings {
		newStrings = append(newStrings, *str)
	}
	return newStrings
}
