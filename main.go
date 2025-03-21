package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
)

type Click struct {
	Timestamp time.Time `json:"timestamp" bun:",pk"`
	BannerID  int       `json:"bannerID" bun:",pk"`
	Count     int       `json:"count"`
}

type Database struct {
	DB *bun.DB
}

func NewDatabase(dsn string) (*Database, error) {
	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	db := bun.NewDB(sqlDB, pgdialect.New())

	return &Database{DB: db}, nil
}

func (db *Database) writeToDB(ctx context.Context, clicksList []Click) error {
	_, err := db.DB.NewInsert().
		Model(&clicksList).
		On("CONFLICT (timestamp, banner_id) DO UPDATE").
		Set("count = click.count + EXCLUDED.count").
		Exec(ctx)
	return err
}

func (db *Database) getStats(ctx context.Context, bannerID int, tsFrom, tsTo time.Time) ([]Click, error) {
	var stats []Click
	err := db.DB.NewSelect().
		Model(&stats).
		Where("banner_id = ? AND timestamp BETWEEN ? AND ?", bannerID, tsFrom, tsTo).
		Scan(ctx)
	return stats, err
}

var (
	clicks      = make(map[int]int)     // Счетчик кликов по баннерам
	clicksMutex sync.Mutex              // Мьютекс для потокобезопасности
	clicksList  = make([]Click, 0, 250) // Список кликов для записи в БД
	db          *Database               // Подключение к БД
)

func main() {
	var err error
	db, err = NewDatabase("postgres://user:password@localhost:5432/mydb?sslmode=disable") // ваша бд
	if err != nil {
		log.Fatal("Ошибка подключения к БД:", err)
	}
	defer db.DB.Close()

	http.HandleFunc("/counter/", counterHandler)
	http.HandleFunc("/stats/", statsHandler)

	go recordClicks()

	fmt.Println("Сервер запущен на порту 8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}

func counterHandler(w http.ResponseWriter, r *http.Request) {
	var id int
	_, err := fmt.Sscanf(r.URL.Path[len("/counter/"):], "%d", &id)
	if err != nil {
		http.Error(w, "Неверный ID баннера", http.StatusBadRequest)
		return
	}

	clicksMutex.Lock()
	clicks[id]++
	clicksMutex.Unlock()

	w.WriteHeader(http.StatusOK)
}

func recordClicks() {
	for {
		time.Sleep(500 * time.Millisecond)

		clicksMutex.Lock()
		for bannerID, count := range clicks {
			if count > 0 {
				clicksList = append(clicksList, Click{
					Timestamp: time.Now().Truncate(time.Minute),
					BannerID:  bannerID,
					Count:     count,
				})
				clicks[bannerID] = 0 // Сбрасываем счетчик
			}
		}
		clicksMutex.Unlock()

		if len(clicksList) > 0 {
			if err := db.writeToDB(context.Background(), clicksList); err != nil {
				log.Println("Ошибка записи в БД:", err)
			}
			clicksList = clicksList[:0]
		}
	}
}

// statsHandler — обработчик для /stats/{id}
func statsHandler(w http.ResponseWriter, r *http.Request) {
	var id int
	_, err := fmt.Sscanf(r.URL.Path[len("/stats/"):], "%d", &id)
	if err != nil {
		http.Error(w, "Неверный ID баннера", http.StatusBadRequest)
		return
	}

	var requestBody struct {
		TsFrom time.Time `json:"tsFrom"`
		TsTo   time.Time `json:"tsTo"`
	}

	if err := json.NewDecoder(r.Body).Decode(&requestBody); err != nil {
		http.Error(w, "Неверный формат данных", http.StatusBadRequest)
		return
	}

	stats, err := db.getStats(context.Background(), id, requestBody.TsFrom, requestBody.TsTo)
	if err != nil {
		http.Error(w, "Ошибка при получении данных", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
