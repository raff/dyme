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

func printMetrics(mm dyme.MMetricsResult, interval int, date bool) {
	values, _ := mm.ByInterval(interval)
	svalues := strings.Replace(fmt.Sprint(values), " ", ",", -1)

	if date {
		fmt.Printf("{%q: %v}\n", mm[0].Date, svalues)
	} else {
		fmt.Println(svalues)
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
	stat := flag.String("stat", "", "stat name")
	query := flag.Bool("q", false, "if true, query database. If false, increment stat")
	compact := flag.Bool("compact", false, "if true, don't print date in range")
	date := flag.String("date", "", "fetch this date only (YYYYMMDD)")
	from := flag.String("from", "", "search from this date")
	to := flag.String("to", "", "search to this date")
	period := flag.Duration("period", 0, "return metrics in units of duration (1 minute minimum)")
	n := flag.Int("n", 1, "increment stat by this value")

	flag.Parse()

	if *stat == "" {
		log.Fatal("missing stat name")
	}

	m, err := dyme.NewMetrics(*table, dyme.Create())
	if err != nil {
		log.Fatal("cannot create Metrics: ", err)
	}

	if !*query && *date == "" && *from == "" && *to == "" {
		curr, err := m.IncrN(*stat, *n)
		if err != nil {
			log.Fatal("cannot increment Metrics: ", err)
		}

		fmt.Println(curr)
		return
	}

	interval := int(*period / time.Minute)

	*date = parseDate(*date)
	*from = parseDate(*from)
	*to = parseDate(*to)

	if *date != "" {

		r, err := m.Get(*stat, *date)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}
		if r == nil {
			log.Println("no Metrics for", *date)
		} else {
			printMetrics(dyme.MMetricsResult{r}, interval, false)
		}
	} else {
		rr, err := m.GetRange(*stat, *from, *to)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}

		printMetrics(rr, interval, !*compact)
	}
}
