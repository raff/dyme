package dyme

import (
	"strconv"
	"strings"
	"time"

	"github.com/raff/dynago"
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

func keyParts(t time.Time) (string, string) {
	if t.IsZero() {
		t = time.Now()
	}

	tdate := t.Format(dateFormat)
	ttime := t.Hour()*60 + t.Minute()

	return tdate, strconv.Itoa(ttime)
}

func metrics(m map[string]interface{}) *MetricsResult {
	res := MetricsResult{Date: m[key_date].(string)}

	for k, v := range m {
		if vv, ok := v.(int); ok && !strings.HasPrefix(k, key_prefix) {
			kk, _ := strconv.Atoi(k)
			res.Values[kk] = vv
		}
	}

	return &res
}

// Metrics is the main object that deals with storing/retriving metrics in DynamoDb
type Metrics struct {
	db    *dynago.DBClient
	table *dynago.TableInstance
}

// MetricsResult is returned by Metrics.Get and GetBatch calls
// it returns the date for the stored metrics and a list of values (1440 entries, one per minute in the selected day)
type MetricsResult struct {
	Date   string
	Values [MINUTES_PER_DAY]int
}

// ByInterval returns a list of total metric for the selected interval in minues
// (i.e. ByInterval(60) will return 24 entries, with the total metrics per hour
func (m MetricsResult) ByInterval(mm int) []int {
	if mm <= 1 {
		return m.Values[:]
	}

	sz := (len(m.Values) + mm - 1) / mm
	ret := make([]int, sz)

	for i, v := range m.Values {
		j := i / mm
		ret[j] += v
	}

	return ret
}

// NewMetrics creates a new Metrics object. It creates the DynamoDB table if it doesn't exist (and create=true)
func NewMetrics(db *dynago.DBClient, table string, create bool) (*Metrics, error) {
	t, err := db.GetTable(table)
	if err == dynago.ERR_NOT_FOUND && create {
		t, err = db.CreateTableInstance(
			table,
			[]dynago.AttributeDefinition{
				dynago.AttributeDefinition{key_id, dynago.STRING_ATTRIBUTE},
				dynago.AttributeDefinition{key_date, dynago.STRING_ATTRIBUTE},
			},
			[]string{
				key_id,
				key_date,
			},
			5,
			5,
			"")
	}

	if err != nil {
		return nil, err
	}

	return &Metrics{db: db, table: t}, nil
}

// Incr increments the specified "stat" at the current slot (equivalent to IncrTime(stat, Now)
func (m *Metrics) Incr(stat string) (int, error) {
	return m.IncrTime(stat, Now)
}

// IncrTime increments the specified "stat" for the time.
// Pass a zero time.Time value (or Now) to increment the current slot
func (m *Metrics) IncrTime(stat string, t time.Time) (int, error) {
	date, offs := keyParts(t)

	item, _, err := m.table.UpdateItem(stat, date,
		"ADD #min :incr",
		dynago.ExpressionAttributeNames(map[string]string{"#min": offs}),
		dynago.ExpressionAttributeValues(map[string]interface{}{":incr": 1}),
		dynago.ReturnValues(dynago.RETURN_UPDATED_NEW))

        if err != nil {
            return 0, err
        }

        return (*item)[offs].(int), nil
}

// Get returns the metrics for the specified date
func (m *Metrics) Get(stat, date string) (*MetricsResult, error) {
	c := "#hash = :hash AND #range = :date"
	names := map[string]string{
		"#hash":  m.table.HashKey().AttributeName,
		"#range": m.table.RangeKey().AttributeName,
	}

	values := map[string]interface{}{
		":hash": stat,
		":date": date,
	}

	q := m.table.Query(nil)
	q.SetConditionExpression(c)
	q.SetAttributeNames(names)
	q.SetAttributeValues(values)

	items, _, _, err := q.Exec(nil)
	if err != nil {
		return nil, err
	}

	return metrics(items[0]), nil
}

// Get returns the metrics for the specified date range.
// You can pass from="" to start at the oldest date, and to="" to end at the newest date
func (m *Metrics) GetRange(stat, from, to string) ([]*MetricsResult, error) {
	c := "#hash = :hash"
	names := map[string]string{
		"#hash":  m.table.HashKey().AttributeName,
		"#range": m.table.RangeKey().AttributeName,
	}

	values := map[string]interface{}{
		":hash": stat,
	}

	if from == "" && to == "" {
		c += " AND #range >= :from"
		values[":from"] = " "
	} else if from != "" && to != "" {
		c += " AND #range BETWEEN :from AND :to"
		values[":from"] = from
		values[":to"] = to
	} else if from != "" {
		c += " AND #range >= :from"
		values[":from"] = from
	} else {
		c += " AND #range <= :to"
		values[":to"] = to
	}

	q := m.table.Query(nil)
	q.SetConditionExpression(c)
	q.SetAttributeNames(names)
	q.SetAttributeValues(values)

	items, _, _, err := q.Exec(nil)
	if err != nil {
		return nil, err
	}

	res := make([]*MetricsResult, len(items))

	for k, i := range items {
		res[k] = metrics(i)
	}

	return res, nil
}
