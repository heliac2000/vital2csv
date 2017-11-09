package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/gocarina/gocsv"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

const (
	ECG_TYPE       = 8
	ACCEL_TYPE     = 1
	ECG_FILE_EXT   = ".ecg_i.csv"
	ACCEL_FILE_EXT = ".acc_i.csv"
	SQL_STATEMENT  = `
SELECT
  (t.ztime + strftime('%s', '2001-01-01 00::00::00')) AS timestamp,
  d.z_fok_timestamp AS zfok_timestamp,
  d.zvalue AS value 
FROM
  ZLOGGEDDATA d INNER JOIN zloggedtime t ON d.ztimestamp = t.z_pk 
WHERE
  d.ztype = :ztype ORDER BY timestamp ASC, zfok_timestamp ASC;
`
)

var ExitCode int = 0

type Ecg struct {
	OriginalTimestamp string  `csv:"time"`
	Ztime             int64   `db:"timestamp" csv:"timestamp"`
	ZFokTimestamp     int64   `db:"zfok_timestamp" csv:"z_fok_timestamp"`
	Zvalue            float64 `db:"value" csv:"value"`
	DetailedTimestamp string  `csv:"detailed_timestamp"`
}

type Accel struct {
	OriginalTimestamp string  `csv:"time"`
	Ztime             int64   `db:"timestamp" csv:"timestamp"`
	ZFokTimestamp     int64   `db:"zfok_timestamp" csv:"z_fok_timestamp"`
	X                 float64 `csv:"x"`
	Y                 float64 `csv:"y"`
	Z                 float64 `db:"value" csv:"z"`
	DetailedTimestamp string  `csv:"detailed_timestamp"`
}

func main() {
	defer func() { os.Exit(ExitCode) }()

	vital, ecgf, accelf := parseCommandLine()

	db, err := sqlx.Connect("sqlite3", vital)
	checkError("Open input file", err)
	defer db.Close()

	stmt, err := db.PrepareNamed(SQL_STATEMENT)
	checkError("Prepare statement", err)
	defer stmt.Close()

	ecg, err := os.OpenFile(ecgf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	checkError("Open output file(ECG)", err)
	defer ecg.Close()

	accel, err := os.OpenFile(accelf, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	checkError("Open output file(Accel)", err)
	defer accel.Close()

	// Stmt is a prepared statement. A Stmt is safe for concurrent use
	// by multiple goroutines.
	var wg sync.WaitGroup
	for t, f := range map[int]*os.File{ECG_TYPE: ecg, ACCEL_TYPE: accel} {
		wg.Add(1)
		go func(t int, f *os.File) {
			defer wg.Done()
			query(stmt, t, f)
		}(t, f)
	}
	wg.Wait()
}

func query(stmt *sqlx.NamedStmt, t int, f *os.File) {
	rows := queryVital(stmt, t)
	defer rows.Close()

	switch t {
	case ECG_TYPE:
		queryECG(rows, f)
	case ACCEL_TYPE:
		queryAcceleration(rows, f)
	}
}

func queryECG(rows *sqlx.Rows, f *os.File) {
	var begin int64
	es := make([]Ecg, 0, 200)

	gocsv.MarshalFile(&es, f) // Write header
	for rows.Next() {
		e := Ecg{}
		err := rows.StructScan(&e)
		checkError("Scan", err)
		if begin < e.Ztime {
			if begin > 0 {
				interpolation(es, e.Ztime)
				gocsv.MarshalWithoutHeaders(&es, f)
				es = es[:0]
			}
			begin = e.Ztime
		}
		e.OriginalTimestamp = time.Unix(e.Ztime, 0).Local().Format("2006-01-02 15:04:05")
		es = append(es, e)
	}
}

func queryAcceleration(rows *sqlx.Rows, f *os.File) {
	var (
		begin int64
		a     [3]Accel
	)
	l, idx := len(a), 0
	as := make([]Accel, 0, 200)

	gocsv.MarshalFile(&as, f) // Write header
	for rows.Next() {
		err := rows.StructScan(&a[idx])
		checkError("Scan", err)
		if idx < l-1 {
			idx++
			continue
		}
		idx = 0

		ztime := a[0].Ztime
		if begin < ztime {
			if begin > 0 {
				interpolation(as, ztime)
				gocsv.MarshalWithoutHeaders(&as, f)
				as = as[:0]
			}
			begin = ztime
		}

		as = append(as, Accel{
			X: a[0].Z, Y: a[1].Z, Z: a[2].Z,
			OriginalTimestamp: time.Unix(ztime, 0).Local().Format("2006-01-02 15:04:05"),
			Ztime:             ztime,
			ZFokTimestamp:     a[0].ZFokTimestamp,
		})
	}
}

func interpolation(v interface{}, end int64) {
	rv := reflect.ValueOf(v)
	l := rv.Len()
	begin := rv.Index(0).FieldByName("Ztime").Int()
	period := float64((end - begin) * 1E+9)
	lf := float64(l)
	for i := 0; i < l; i++ {
		rv.Index(i).FieldByName("DetailedTimestamp").SetString(
			time.Unix(begin, int64(float64(i)*period/lf)).Local().Format("2006-01-02 15:04:05.000000000"))
	}
}

func queryVital(stmt *sqlx.NamedStmt, ztype int) *sqlx.Rows {
	rows, err := stmt.Queryx(map[string]interface{}{"ztype": ztype})
	checkError("Query", err)
	return rows
}

func parseCommandLine() (string, string, string) {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, `
Usage of %s:
  %s [options] vital_data
`, path.Base(os.Args[0]), os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\n")
	}

	var d string
	flag.StringVar(&d, "d", "", "Output directory for csv data")
	flag.StringVar(&d, "outDir", "", "Output directory for csv data(long option)")
	flag.Parse()

	v := flag.Args()
	if len(v) != 1 {
		flag.Usage()
		os.Exit(ExitCode)
	}

	if _, err := os.Stat(v[0]); os.IsNotExist(err) {
		log.Fatal(err)
	}

	vital := v[0]
	base := filepath.Base(vital)
	ecg := filepath.Join(d, strings.TrimSuffix(base, filepath.Ext(base))+ECG_FILE_EXT)
	accel := filepath.Join(d, strings.TrimSuffix(base, filepath.Ext(base))+ACCEL_FILE_EXT)

	return vital, ecg, accel
}

func checkError(msg string, err error) {
	if err != nil {
		log.Print(msg+": ", err)
		ExitCode = 1
		runtime.Goexit()
	}
}
