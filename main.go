package main

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/jinzhu/gorm/dialects/postgres"
)

// const dbConnString = "user=username host=localhost dbname=dbname sslmode=disable password=password"

func main() {
	start := time.Now()
	listFile, err := readCurrentDir()
	if err != nil {
		log.Println(err)
	}

	log.Println("files will be insert", listFile)
	log.Println("connecting to db ...")

	db, err := sql.Open("postgres", dbConnString)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("connected")

	for _, filename := range listFile {

		log.Println("inserting " + filename + " to database, please wait ...")

		records := ReadCSV("csv/" + filename)
		totalLoop := len(records) / 1000
		modLoop := len(records) % 1000
		// var arrInsert []int

		dbname := strings.Split(filename, ".")
		header := strings.Join(records[0], ",")

		c := []string{}
		for i := 0; i < len(records[0]); i++ {
			c = append(c, "?")
		}
		col := strings.Join(c, ",")

		log.Println("inserting data will do in batch data, every batch will be insert 1000 row data")
		log.Println(filename + " will be devide on " + strconv.Itoa(totalLoop+1) + " batch data")

		arr := 1
		for k := 0; k < totalLoop; k++ {
			log.Println("batch " + strconv.Itoa(k+1))
			vals := []interface{}{}
			for i := arr; i < arr+1000; i++ {
				for _, v := range records[i] {
					// log.Println(v)
					vals = append(vals, v)
				}
			}

			sqlStr := "INSERT INTO " + dbname[0] + "(" + header + ") VALUES %s"

			// trim the last ,
			sqlStr = strings.TrimSuffix(sqlStr, ",")

			// Replacing ? with $n for postgres
			sqlStr = ReplaceSQL(sqlStr, "("+col+")", 1000)

			// prepare the statement
			stmt, err := db.Prepare(sqlStr)
			if err != nil {
				log.Println("PREPARE", err)
			}
			// //format all vals at once
			_, err = stmt.Exec(vals...)
			if err != nil {
				log.Println("EXEC", err)
			}
			arr = arr + 1000

			// log.Println(filename + " sucessfully inserted")
		}

		if modLoop < 1000 {
			log.Println("last batch")
			vals := []interface{}{}
			for i := totalLoop*1000 + 1; i < (totalLoop*1000)+modLoop; i++ {
				for _, v := range records[i] {
					vals = append(vals, v)
				}
			}

			sqlStr := "INSERT INTO " + dbname[0] + "(" + header + ") VALUES %s"

			// trim the last ,
			sqlStr = strings.TrimSuffix(sqlStr, ",")

			// Replacing ? with $n for postgres
			sqlStr = ReplaceSQL(sqlStr, "("+col+")", modLoop-1)

			// prepare the statement
			stmt, err := db.Prepare(sqlStr)
			if err != nil {
				log.Println("PREPARE", err)
			}
			// //format all vals at once
			_, err = stmt.Exec(vals...)
			if err != nil {
				log.Println("EXEC", err)
			}
		}
		err := MoveFile("csv/"+filename, "csv_inserted/"+filename)
		if err != nil {
			log.Fatal(err)
		}
	}
	duration := time.Since(start)
	fmt.Println("done in", int64(duration.Milliseconds()), "ms")
}

func MoveFile(old, new string) error {
	err := os.Rename(old, new)
	if err != nil {
		log.Fatal(err)
	}
	return err
}

func ReplaceSQL(stmt, pattern string, len int) string {
	pattern += ","
	stmt = fmt.Sprintf(stmt, strings.Repeat(pattern, len))
	n := 0
	for strings.IndexByte(stmt, '?') != -1 {
		n++
		param := "$" + strconv.Itoa(n)
		stmt = strings.Replace(stmt, "?", param, 1)
	}
	return strings.TrimSuffix(stmt, ",")
}

func ReadCSV(filePath string) [][]string {

	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal("Unable to read input file "+filePath, err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	return records
}

func readCurrentDir() ([]string, error) {

	file, err := os.Open("./csv")
	if err != nil {
		log.Fatalf("failed opening directory: %s", err)
	}
	defer file.Close()

	list, err := file.Readdirnames(0) // 0 to read all files and folders
	if err != nil {
		log.Println(err)
	}
	return list, err
}
