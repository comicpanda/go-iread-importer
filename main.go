package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/lib/pq"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {

	tablename := flag.String("t", "iread", "tablename")
	username := flag.String("u", "root", "username")
	password := flag.String("p", "", "password")

	flag.Parse()

	if len(flag.Args()) < 1 {
		log.Println("Usage: iread-importer -u=root -p=password out1.csv")
		os.Exit(1)
	}

	filename := flag.Args()[0]
	log.Printf("%s\n", filename)

	db, err := sql.Open("postgres", fmt.Sprintf("postgres://%s:%s@127.0.0.1/comicpanda?sslmode=disable", *username, *password))
	isError(err)
	defer db.Close()

	inFile, _ := os.Open(filename)
	defer inFile.Close()
	scanner := bufio.NewScanner(inFile)
	scanner.Split(bufio.ScanLines)

	var i = -1
	var query = fmt.Sprintf("INSERT INTO %s (user_id, series_id, episode_id, read_at) VALUES ($1, $2, $3, $4)", *tablename)

	for scanner.Scan() {
		i++
		if i == 0 {
			continue
		}
		row := strings.Replace(scanner.Text(), "\"", "", -1)
		data := strings.Split(row, ",")
		stmt, err := db.Prepare(query)
		isError(err)
		if len(data[3]) == 0 {
			fmt.Printf(data[3])
			data[3] = "1418381086191"
		}
		stamp, _ := strconv.ParseInt(data[3], 10, 64)
		readAt := time.Unix(stamp/1000, 0)
		_, err = stmt.Exec(data[0], data[2], data[1], readAt)
		isError(err)
		stmt.Close()
	}
}

func isError(err error) {
	if err != nil {
		log.Println(err)
	}
}
