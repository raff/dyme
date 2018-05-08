package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/raff/dyme"
)

type Format int

const (
	Start Format = iota
	Labels
	Compact
)

func printMetrics(mm dyme.MMetricsResult, interval, period int, format Format, nz bool) {
	values, _ := mm.ByInterval(interval)
	if period > 0 && period < len(values) {
		start := len(values) - period
		values = values[start:]
	}

	if nz {
		for i := len(values) - 1; i >= 0; i-- {
			if values[i] != 0 {
				values = values[:i+1]
				break
			}
		}
	}

	svalues := strings.Replace(fmt.Sprint(values), " ", ",", -1)

	switch format {
	case Compact:
		fmt.Println(svalues)

	case Start:
		if len(mm) == 0 {
			fmt.Println("{}")
		} else {
			fmt.Printf("{%q: %v}\n", mm[0].Date, svalues)
		}

	case Labels:
		// not really implemented yet
		if len(mm) == 0 {
			fmt.Println("{}")
		} else {
			fmt.Printf("{%q: %v}\n", mm[0].Date, svalues)
		}
	}
}

func parseDate(date string) string {
	if date != "" {
		d, err := strconv.Atoi(date)
		if err != nil {
			log.Fatal("invalid date")
		}

		if d < 500 {
			// days before today
			date = dyme.DateKey(time.Now().AddDate(0, 0, -d))
		}
	}

	return date
}

func main() {
	table := flag.String("table", "stats", "table name")
	profile := flag.String("profile", "", "aws profile")
	stat := flag.String("stat", "", "stat name")
	op := flag.String("op", "get", "get or incr/set")
	sformat := flag.String("format", "", "output format: labels, compact, start")
	date := flag.String("date", "", "fetch this date only (YYYYMMDD)")
	from := flag.String("from", "", "search from this date")
	to := flag.String("to", "", "search to this date")
	interval := flag.Duration("interval", 0, "return metrics in units of duration (1 minute minimum)")
	period := flag.Int("period", 0, "return period (number of values)")
	n := flag.Int("n", 1, "increment stat by this value")
	nz := flag.Bool("z", false, "remove trailing zeroes")

	flag.Parse()

	if *stat == "" {
		log.Fatal("missing stat name")
	}

	m, err := dyme.NewMetrics(*table, dyme.Create(), dyme.Profile(*profile))
	if err != nil {
		log.Fatal("cannot create Metrics: ", err)
	}

	if *op != "get" && *date == "" && *from == "" && *to == "" {
		curr, err := m.IncrN(*stat, *n)
		if err != nil {
			log.Fatal("cannot increment Metrics: ", err)
		}

		fmt.Println(curr)
		return
	}

	iinterval := int(*interval / time.Minute)

	*date = parseDate(*date)
	*from = parseDate(*from)
	*to = parseDate(*to)

	var format Format

	switch *sformat {
	case "compact":
		format = Compact

	case "labels":
		format = Labels

	default:
		format = Start
	}

	if *date != "" {
		r, err := m.Get(*stat, *date)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}
		if r == nil {
			log.Println("no Metrics for", *date)
		} else {
			printMetrics(dyme.MMetricsResult{r}, iinterval, *period, format, *nz)
		}
	} else {
		rr, err := m.GetRange(*stat, *from, *to)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}

		printMetrics(rr, iinterval, *period, format, *nz)
	}
}
