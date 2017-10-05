package dynamodb

import (
	"testing"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/eawsy/aws-lambda-go-event/service/lambda/runtime/event/dynamodbstreamsevt"
	"strings"
	"regexp"
)

func TestConvert(t *testing.T) {
	//given
	input := make(map[string]*dynamodbstreamsevt.AttributeValue)
	list := []*dynamodbstreamsevt.AttributeValue{{N: "1"}, {N: "13"}, {N: "42"}}
	input["aString"] = &dynamodbstreamsevt.AttributeValue{S: "kopytko"}
	input["aList"] = &dynamodbstreamsevt.AttributeValue{L: list}
	input["aBool"] = &dynamodbstreamsevt.AttributeValue{BOOL: true}
	//when
	output := Convert(input)
	//then
	if len(output) != 3 {
		t.Fail()
	}
	assertAttributeValue(t, output["aString"], "{ S: \"kopytko\"}")
	assertAttributeValue(t, output["aList"], "{ L: [{ N: \"1\" },{ N: \"13\" },{ N: \"42\" }]}")
	assertAttributeValue(t, output["aBool"], "{ BOOL: true}")
}

func TestConvertEmptyCollections(t *testing.T) {
	//given
	input := make(map[string]*dynamodbstreamsevt.AttributeValue)
	input["aList"] = &dynamodbstreamsevt.AttributeValue{L: []*dynamodbstreamsevt.AttributeValue{}}
	input["aMap"] = &dynamodbstreamsevt.AttributeValue{M: make(map[string]*dynamodbstreamsevt.AttributeValue)}
	input["aBinarySet"] = &dynamodbstreamsevt.AttributeValue{BS: [][]byte{}}
	input["aNumberSet"] = &dynamodbstreamsevt.AttributeValue{NS: []string{}}
	input["aStringSet"] = &dynamodbstreamsevt.AttributeValue{SS: []string{}}
	//when
	output := Convert(input)
	//then
	assertAttributeValue(t, output["aList"], "{ L: []}")
	assertAttributeValue(t, output["aMap"], "{ M: { }}")
	assertAttributeValue(t, output["aBinarySet"], "{ BS: []}")
	assertAttributeValue(t, output["aNumberSet"], "{ NS: []}")
	assertAttributeValue(t, output["aStringSet"], "{ SS: []}")
}

func TestConvertNull(t *testing.T) {
	//given
	input := make(map[string]*dynamodbstreamsevt.AttributeValue)
	input["aNull"] = &dynamodbstreamsevt.AttributeValue{NULL: true}
	//when
	output := Convert(input)
	//then
	assertAttributeValue(t, output["aNull"], "{ NULL: true}")
}

func TestUnconvert(t *testing.T) {
	//given
	input := make(map[string]*dynamodb.AttributeValue)
	list := []*dynamodb.AttributeValue{{N: stringPtr("1")}, {N: stringPtr("13")}, {N: stringPtr("42")}}
	input["aString"] = &dynamodb.AttributeValue{S: stringPtr("kopytko")}
	input["aList"] = &dynamodb.AttributeValue{L: list}
	input["aBool"] = &dynamodb.AttributeValue{BOOL: boolPtr(true)}
	//when
	output := Unconvert(input)
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

func assertAttributeValue(t *testing.T, attr *dynamodb.AttributeValue, s string) {
	s1 := clearString(attr.String())
	s2 := clearString(s)
	if s1 != s2 {
		msg := s1 + " does not equal " + s2
		t.Errorf(msg)
	}
}

func clearString(s string) string {
	multipleWhitespacesRegex := regexp.MustCompile(`[\s\p{Zs}]{2,}`)
	final := multipleWhitespacesRegex.ReplaceAllString(s, " ")
	trimmed := strings.Replace(strings.TrimSpace(final), "\n", "", -1)
	return trimmed
}

func stringPtr(s string) *string{
	return &s
}

func boolPtr(s bool) *bool{
	return &s
}
