package dyme

import (
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
)

const (
	MINUTES_PER_DAY = 24 * 60

	key_prefix = "_"
	key_id     = "_id"
	key_date   = "_date"
	dateFormat = "20060102"
)

var (
	Now time.Time
)

func DateKey(t time.Time) string {
	if t.IsZero() {
		t = time.Now()
	}

	return t.Format(dateFormat)
}

func keyParts(t time.Time) (string, string) {
	if t.IsZero() {
		t = time.Now()
	}

	tdate := t.Format(dateFormat)
	ttime := t.Hour()*60 + t.Minute()

	return tdate, strconv.Itoa(ttime)
}

func Nint(s *string) int {
	n, _ := strconv.Atoi(*s)
	return n
}

func intN(n int) *string {
	return aws.String(strconv.Itoa(n))
}

func metrics(m map[string]*dynamodb.AttributeValue) *MetricsResult {
	if _, ok := m[key_date]; !ok {
		return nil
	}

	date := m[key_date].S
	res := MetricsResult{Date: *date}

	for k, v := range m {
		if !strings.HasPrefix(k, key_prefix) && v != nil {
			kk, _ := strconv.Atoi(k)
			res.Values[kk] = Nint(v.N)
		}
	}

	return &res
}

// Metrics is the main object that deals with storing/retriving metrics in DynamoDb
type Metrics struct {
	db    *dynamodb.DynamoDB
	table string
}

// MetricsResult is returned by Metrics.Get and GetBatch calls
// it returns the date for the stored metrics and a list of values (1440 entries, one per minute in the selected day)
type MetricsResult struct {
	Date   string
	Values [MINUTES_PER_DAY]int
}

// ByInterval returns a list of total metrics for the selected interval in minues
// (i.e. ByInterval(60) will return 24 entries, with the total metrics per hour
func (m MetricsResult) ByInterval(mm int) (values []int, max int) {
	if mm <= 1 {
		for _, v := range m.Values {
			if v > max {
				max = v
			}
		}

		values = m.Values[:]
		return
	}

	sz := (len(m.Values) + mm - 1) / mm
	values = make([]int, sz)

	for i, v := range m.Values {
		j := i / mm
		values[j] += v

		if values[j] > max {
			max = values[j]
		}
	}

	return
}

type MMetricsResult []*MetricsResult

// ByInterval returns a list of total metrics for the selected interval in minues
// (i.e. ByInterval(60) will return 24 entries, with the total metrics per hour
func (l MMetricsResult) ByInterval(mm int) (values []int, max int) {
	for _, m := range l {
		v, m := m.ByInterval(mm)
		values = append(values, v...)
		if m > max {
			max = m
		}
	}

	return
}

type config struct {
	region   string
	profile  string
	create   bool
	readCap  int64
	writeCap int64
}

type MetricsOption func(c *config)

func Region(r string) MetricsOption {
	return func(c *config) {
		c.region = r
	}
}

func Profile(p string) MetricsOption {
	return func(c *config) {
		c.profile = p
	}
}

func Create() MetricsOption {
	return func(c *config) {
		c.create = true
	}
}

func Capacity(rc, wc int64) MetricsOption {
	return func(c *config) {
		c.readCap = rc
		c.writeCap = wc
	}
}

// NewMetrics creates a new Metrics object. It creates the DynamoDB table if it doesn't exist (and create=true)
func NewMetrics(table string, options ...MetricsOption) (*Metrics, error) {
	mconf := config{
		region:   "us-east-1",
		profile:  "",
		create:   false,
		readCap:  5,
		writeCap: 5,
	}

	for _, setOption := range options {
		setOption(&mconf)
	}

	sess, err := session.NewSessionWithOptions(session.Options{
		Profile: mconf.profile,
		Config:  aws.Config{Region: aws.String(mconf.region)},
	})
	if err != nil {
		return nil, err
	}

	db := dynamodb.New(sess)

	_, err = db.DescribeTable(&dynamodb.DescribeTableInput{TableName: aws.String(table)})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok &&
			aerr.Code() == dynamodb.ErrCodeResourceNotFoundException && mconf.create {

			_, err = db.CreateTable(&dynamodb.CreateTableInput{
				TableName: aws.String(table),

				AttributeDefinitions: []*dynamodb.AttributeDefinition{
					{AttributeName: aws.String(key_id), AttributeType: aws.String("S")},
					{AttributeName: aws.String(key_date), AttributeType: aws.String("S")},
				},

				KeySchema: []*dynamodb.KeySchemaElement{
					{AttributeName: aws.String(key_id), KeyType: aws.String("HASH")},
					{AttributeName: aws.String(key_date), KeyType: aws.String("RANGE")},
				},

				ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(mconf.readCap),
					WriteCapacityUnits: aws.Int64(mconf.writeCap),
				}})

		}

		if err != nil {
			return nil, err
		}
	}

	return &Metrics{db: db, table: table}, nil
}

// Incr increments the specified "stat" at the current slot (equivalent to IncrTime(stat, Now)
func (m *Metrics) Incr(stat string) (count int, err error) {
	if m != nil {
		count, err = m.IncrTime(stat, 1, Now)
	}

	return
}

// IncrN increments the specified "stat" at the current slot by the specified amount n
func (m *Metrics) IncrN(stat string, n int) (count int, err error) {
	if m != nil {
		count, err = m.IncrTime(stat, n, Now)
	}

	return
}

// IncrTime increments the specified "stat" for the time.
// Pass a zero time.Time value (or Now) to increment the current slot
func (m *Metrics) IncrTime(stat string, n int, t time.Time) (int, error) {
	date, offs := keyParts(t)

	res, err := m.db.UpdateItem(&dynamodb.UpdateItemInput{
		TableName: aws.String(m.table),

		Key: map[string]*dynamodb.AttributeValue{
			key_id:   {S: aws.String(stat)},
			key_date: {S: aws.String(date)},
		},

		UpdateExpression:          aws.String("ADD #min :incr"),
		ExpressionAttributeNames:  map[string]*string{"#min": aws.String(offs)},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":incr": {N: intN(n)}},

		ReturnValues: aws.String("UPDATED_NEW"),
	})

	if err != nil {
		return 0, err
	}

	return Nint(res.Attributes[offs].N), nil
}

// Get returns the metrics for the specified date
func (m *Metrics) Get(stat, date string) (*MetricsResult, error) {
	res, err := m.db.GetItem(&dynamodb.GetItemInput{
		TableName: aws.String(m.table),
		Key: map[string]*dynamodb.AttributeValue{
			key_id:   {S: aws.String(stat)},
			key_date: {S: aws.String(date)},
		},
	})

	if err != nil {
		return nil, err
	}

	return metrics(res.Item), nil
}

// Get returns the metrics for the specified date range.
// You can pass from="" to start at the oldest date, and to="" to end at the newest date
func (m *Metrics) GetRange(stat, from, to string) (MMetricsResult, error) {

	cond := expression.Key(key_id).Equal(expression.Value(stat))
	if from == "" && to == "" {
		cond = expression.KeyAnd(cond, expression.Key(key_date).GreaterThanEqual(expression.Value(" ")))
	} else if from != "" && to != "" {
		cond = expression.KeyAnd(cond, expression.Key(key_date).Between(expression.Value(from),
			expression.Value(to)))
	} else if from != "" {
		cond = expression.KeyAnd(cond, expression.Key(key_date).GreaterThanEqual(expression.Value(from)))
	} else {
		cond = expression.KeyAnd(cond, expression.Key(key_date).LessThanEqual(expression.Value(from)))
	}

	expr, err := expression.NewBuilder().WithKeyCondition(cond).Build()
	if err != nil {
		return nil, err
	}

	res, err := m.db.Query(&dynamodb.QueryInput{
		TableName:                 aws.String(m.table),
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
	})
	if err != nil {
		return nil, err
	}

	mres := make([]*MetricsResult, len(res.Items))

	for k, i := range res.Items {
		mres[k] = metrics(i)
	}

	return mres, nil
}
