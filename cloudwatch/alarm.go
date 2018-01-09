package cloudwatch

import (
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"time"
	"fmt"
)

// Client structure defines a wraps official AWS Cloudwatch client and provides set of utility methods that operates on cloudwatch alarms.
type Client struct {
	Cloudwatch *cw.CloudWatch
}

// CloneAndPutMetricAlarm retrieves details of existing alarm by name and updates it using provided function
func (client *Client) CloneAndPutMetricAlarm(alarmName string, update func(alarm cw.MetricAlarm) cw.MetricAlarm) (cw.PutMetricAlarmOutput, error) {
	return client.cloneAndPutMetricAlarm(alarmName, client.getAlarm, update)
}

// CloneByPrefixAndPutMetricAlarm retrieves details of existing alarm by prefix and updates it using provided function
func (client *Client) CloneByPrefixAndPutMetricAlarm(alarmPrefix string, update func(alarm cw.MetricAlarm) cw.MetricAlarm) (cw.PutMetricAlarmOutput, error) {
	return client.cloneAndPutMetricAlarm(alarmPrefix, client.getAlarmByPrefix, update)
}

// PutMetric sends dimension metric data to cloudwatch
func (client *Client) PutMetric(namespace string, metricName string, dimensionName string, dimensionValue string, value float64, unit string) (cw.PutMetricDataOutput, error) {
	now := time.Now()
	dimension := cw.Dimension{Name: &dimensionName, Value: &dimensionValue}
	metricDataInput := &cw.PutMetricDataInput{
		MetricData: []*cw.MetricDatum{
			{
				MetricName: &metricName,
				Dimensions: []*cw.Dimension{&dimension},
				Timestamp:  &now,
				Value:      &value,
				Unit:       &unit,
			},
		},
		Namespace: &namespace,
	}
	res, err := client.Cloudwatch.PutMetricData(metricDataInput)
	return *res, err
}

// GetLastNMinMetrics retrieves sum of data points that occurred within last N minutes
func (client *Client) GetLastNMinMetrics(timeLength int64, namespace string, metricName string, dimensionName string,
	dimensionValue string, period int64) ([]float64, error) {
	end := time.Now()
	start := end.Add(-1 *time.Duration(timeLength) * time.Minute)
	dimension := cw.Dimension{Name: &dimensionName, Value: &dimensionValue}
	sum := "Sum"
	req := &cw.GetMetricStatisticsInput{
		EndTime:    &end,
		MetricName: &metricName,
		Namespace:  &namespace,
		Period:     &period,
		StartTime:  &start,
		Dimensions: []*cw.Dimension{&dimension},
		Statistics: []*string{&sum},
	}
	res, err := client.Cloudwatch.GetMetricStatistics(req)
	sums := make([]float64, 0)
	for _, dp := range res.Datapoints {
		sums = append(sums, *dp.Sum)
	}
	return sums, err
}

// SetAlarmToOk set alarm state to status OK
func (client *Client) SetAlarmToOk(alarmName string) (cw.SetAlarmStateOutput, error) {
	reason := "handled"
	stateOk := "OK"
	req := &cw.SetAlarmStateInput{AlarmName: &alarmName, StateReason: &reason, StateValue: &stateOk}
	res, err := client.Cloudwatch.SetAlarmState(req)
	return *res, err
}

func (client *Client) cloneAndPutMetricAlarm(alarmNameOrPrefix string, getAlarm func(nameOrPrefix string) (metricAlarm cw.MetricAlarm, err error), update func(alarm cw.MetricAlarm) cw.MetricAlarm) (cw.PutMetricAlarmOutput, error) {
	var output *cw.PutMetricAlarmOutput
	alarmDetails, alarmErr := getAlarm(alarmNameOrPrefix)
	if alarmErr != nil {
		return *output, alarmErr
	}
	updatedAlarm := update(alarmDetails)
	input := clonePutMetricAlarmInput(updatedAlarm)
	output, err := client.Cloudwatch.PutMetricAlarm(&input)
	return *output, err
}

func clonePutMetricAlarmInput(updatedAlarm cw.MetricAlarm) cw.PutMetricAlarmInput {
	return cw.PutMetricAlarmInput{
		ActionsEnabled:                   updatedAlarm.ActionsEnabled,
		AlarmActions:                     updatedAlarm.AlarmActions,
		AlarmDescription:                 updatedAlarm.AlarmDescription,
		AlarmName:                        updatedAlarm.AlarmName,
		ComparisonOperator:               updatedAlarm.ComparisonOperator,
		Dimensions:                       updatedAlarm.Dimensions,
		EvaluateLowSampleCountPercentile: updatedAlarm.EvaluateLowSampleCountPercentile,
		EvaluationPeriods:                updatedAlarm.EvaluationPeriods,
		ExtendedStatistic:                updatedAlarm.ExtendedStatistic,
		InsufficientDataActions:          updatedAlarm.InsufficientDataActions,
		MetricName:                       updatedAlarm.MetricName,
		Namespace:                        updatedAlarm.Namespace,
		OKActions:                        updatedAlarm.OKActions,
		Period:                           updatedAlarm.Period,
		Statistic:                        updatedAlarm.Statistic,
		Threshold:                        updatedAlarm.Threshold,
		TreatMissingData:                 updatedAlarm.TreatMissingData,
		Unit:                             updatedAlarm.Unit,
	}
}

func (client *Client) getAlarm(alarmName string) (metricAlarm cw.MetricAlarm, err error) {

	alarms := []*string{&alarmName}
	maxRecords := int64(1)
	req := cw.DescribeAlarmsInput{AlarmNames: alarms, MaxRecords: &maxRecords}
	res, _ := client.Cloudwatch.DescribeAlarms(&req)
	if len(res.MetricAlarms) > 0 {
		metricAlarm = *res.MetricAlarms[0]
		return metricAlarm, nil
	}
	return metricAlarm, fmt.Errorf("MetricAlarms are empty")
}

func (client *Client) getAlarmByPrefix(alarmNamePrefix string) (metricAlarm cw.MetricAlarm, err error) {

	maxRecords := int64(1)
	req := cw.DescribeAlarmsInput{AlarmNamePrefix: &alarmNamePrefix, MaxRecords: &maxRecords}
	res, _ := client.Cloudwatch.DescribeAlarms(&req)
	if len(res.MetricAlarms) > 0 {
		metricAlarm = *res.MetricAlarms[0]
		return metricAlarm, nil
	}
	return metricAlarm, fmt.Errorf("MetricAlarms are empty")
}


