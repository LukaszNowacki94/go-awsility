package cloudwatch

import (
	cw "github.com/aws/aws-sdk-go/service/cloudwatch"
	"time"
)

// Client structure defines a wraps official AWS Cloudwatch client and provides set of utility methods that operates on cloudwatch alarms.
type Client struct {
	Cloudwatch *cw.CloudWatch
}

// CloneAndPutMetricAlarm retrieves details of existing alarm and updates it using provided function
func (alarm *Client) CloneAndPutMetricAlarm(alarmName string, update func(alarm cw.MetricAlarm) cw.MetricAlarm) (cw.PutMetricAlarmOutput, error) {
	alarmDetails := alarm.getAlarm(alarmName)
	updatedAlarm := update(alarmDetails)
	input := &cw.PutMetricAlarmInput{
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
	res, err := alarm.Cloudwatch.PutMetricAlarm(input)
	return *res, err
}

// PutMetric sends dimension metric data to cloudwatch
func (alarm *Client) PutMetric(namespace string, metricName string, dimensionName string, dimensionValue string, value float64, unit string) (cw.PutMetricDataOutput, error) {
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
	res, err := alarm.Cloudwatch.PutMetricData(metricDataInput)
	return *res, err
}

// GetLast5MinMetrics retrieves sum of data points that occurred within last 5 minutes
func (alarm *Client) GetLast5MinMetrics(namespace string, metricName string, dimensionName string, dimensionValue string, period int64) ([]float64, error) {
	end := time.Now()
	start := end.Add(-5 * time.Minute)
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
	res, err := alarm.Cloudwatch.GetMetricStatistics(req)
	sums := make([]float64, 0)
	for _, dp := range res.Datapoints {
		sums = append(sums, *dp.Sum)
	}
	return sums, err
}

// SetAlarmToOk set alarm state to status OK
func (alarm *Client) SetAlarmToOk(alarmName string) (cw.SetAlarmStateOutput, error) {
	reason := "handled"
	stateOk := "Ok"
	req := &cw.SetAlarmStateInput{AlarmName: &alarmName, StateReason: &reason, StateValue: &stateOk}
	res, err := alarm.Cloudwatch.SetAlarmState(req)
	return *res, err
}

func (alarm *Client) getAlarm(alarmName string) cw.MetricAlarm {
	alarms := []*string{&alarmName}
	maxRecords := int64(1)
	req := cw.DescribeAlarmsInput{AlarmNames: alarms, MaxRecords: &maxRecords}
	res, _ := alarm.Cloudwatch.DescribeAlarms(&req)
	metricAlarm := *res.MetricAlarms[0]
	return metricAlarm
}
