package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	//"encoding/json"
	"time"

	"github.com/raff/dyme"
)

func main() {
	table := flag.String("table", "stats", "table name")
	stat := flag.String("stat", "", "stat name")
	query := flag.Bool("q", false, "if true, query database. If false, increment stat")
	date := flag.String("date", "", "fetch this date only")
	from := flag.String("from", "", "search from this date")
	to := flag.String("to", "", "search to this date")
	period := flag.Duration("period", 0, "return metrics in units of duration (1 minute minimum)")
	n := flag.Int("n", 1, "increment stat by this value")
	creds := flag.String("creds", "", "DynamoDB credentials (key:secret)")

	flag.Parse()

	if *stat == "" {
		log.Fatal("missing stat name")
	}

	ks := strings.Split(*creds, ":")
	if len(ks) == 2 {
		os.Setenv("AWS_ACCESS_KEY", ks[0])
		os.Setenv("AWS_SECRET_KEY", ks[1])
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

	if *date != "" {
		r, err := m.Get(*stat, *date)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}
		if r == nil {
			log.Println("no Metrics for", *date)
		} else {
			fmt.Println(r.Date, r.ByInterval(interval))
		}
	} else {
		rr, err := m.GetRange(*stat, *from, *to)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}
		for _, r := range rr {
			fmt.Println(r.Date, r.ByInterval(interval))
		}
	}
}
