package database

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/joho/godotenv/autoload"
	_ "github.com/lib/pq"
)

type Service interface {
	Health() map[string]string
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(command string, args ...interface{}) (sql.Result, error)
	Begin() (*sql.Tx, error)
	Ping() error
	Close() error
}

type service struct {
	db *sql.DB
}

var (
	database = os.Getenv("DB_DATABASE")
	password = os.Getenv("DB_PASSWORD")
	username = os.Getenv("DB_USERNAME")
	port     = os.Getenv("DB_PORT")
	host     = os.Getenv("DB_HOST")
)

func New() Service {
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", username, password, host, port, database)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}
	s := &service{db: db}
	return s
}

func (s *service) Health() map[string]string {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err := s.db.PingContext(ctx)
	if err != nil {
		log.Fatalf(fmt.Sprintf("db down: %v", err))
	}

	return map[string]string{
		"message": "It's healthy",
	}
}

func (s *service) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return s.db.Query(query, args...)
}

func (s *service) Exec(command string, args ...interface{}) (sql.Result, error) {
	return s.db.Exec(command, args...)
}

func (s *service) Begin() (*sql.Tx, error) {
	return s.db.Begin()
}

func (s *service) Ping() error {
	return s.db.Ping()
}

func (s *service) Close() error {
	return s.db.Close()
}
