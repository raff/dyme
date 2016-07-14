package main

import (
	"flag"
	"log"
	"os"
	"strings"

	"github.com/raff/dyme"
)

func main() {
	table := flag.String("table", "stats", "table name")
	stat := flag.String("stat", "", "stat name")
	query := flag.Bool("q", false, "if true, query database. If false, increment stat")
	date := flag.String("date", "", "fetch this date only")
	from := flag.String("from", "", "search from this date")
	to := flag.String("to", "", "search to this date")
	n := flag.Int("n", 0, "return metrics per `n` minutes")
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

	m, err := dyme.NewMetrics(*table, true)
	if err != nil {
		log.Fatal("cannot create Metrics: ", err)
	}

	if !*query {
		curr, err := m.Incr(*stat)
		if err != nil {
			log.Fatal("cannot increment Metrics: ", err)
		}

		log.Println(curr)
		return
	}

	if *date != "" {
		r, err := m.Get(*stat, *date)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}
		log.Println(r.Date, r.ByInterval(*n))
	} else {
		rr, err := m.GetRange(*stat, *from, *to)
		if err != nil {
			log.Fatal("cannot get Metrics: ", err)
		}
		for _, r := range rr {
			log.Println(r.Date, r.ByInterval(*n))
		}
	}
}
