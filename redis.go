/*
Работа с redis
*/
package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/alrusov/config"
	"github.com/alrusov/misc"

	"github.com/go-redis/redis/v8"
)

//----------------------------------------------------------------------------------------------------------------------------//

type (
	// Конфигурация
	Config struct {
		Host     string        `toml:"host"`     // Хост
		Password string        `toml:"password"` // Пароль
		DB       int           `toml:"db"`       // База (номер)
		TimeoutS string        `toml:"timeout"`  // Строчное представление таймаута
		Timeout  time.Duration `toml:"-"`        // Таймаут

	}

	// Соединение
	Connection struct {
		client *redis.Client // Клиент redis
	}

	// zrange
	ZMembers []*redis.Z
)

const (
	// Максимальное количество строк, сохраняемых за один раз (ограничение redis)
	MaxHSaveSize = 512*1024 - 1
)

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка валидности Config
func (x *Config) Check(cfg interface{}) (err error) {
	msgs := misc.NewMessages()

	if x.Host == "" {
		msgs.Add(`host: is empty or undefined`)
	}

	x.Timeout, err = misc.Interval2Duration(x.TimeoutS)
	if err != nil {
		msgs.Add(`timeout: %s`, err)
	}

	if x.Timeout <= 0 {
		x.Timeout = config.ClientDefaultTimeout
	}

	return msgs.Error()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Создать соединение
func New(cfg *Config) *Connection {
	return &Connection{
		client: redis.NewClient(
			&redis.Options{
				Addr:        cfg.Host,
				Password:    cfg.Password,
				DB:          cfg.DB,
				DialTimeout: cfg.Timeout,
				ReadTimeout: cfg.Timeout,
			},
		),
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

// Закрыть соединение
func (r *Connection) Close() {
	if r.client != nil {
		r.client.Close()
		r.client = nil
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

// Проверка соединения
func (r *Connection) Ping() error {
	res := r.client.Ping(context.Background())
	pong, err := res.Result()
	if err == nil {
		if pong == "PONG" {
			err = nil
		} else {
			err = fmt.Errorf("ping error")
		}
	}
	return err
}

//----------------------------------------------------------------------------------------------------------------------------//

// Сохранить данные в hashtable
func (r *Connection) HSave(key string, data []string) error {
	ln := len(data)

	if ln == 0 {
		return nil
	}

	if ln%2 != 0 {
		return fmt.Errorf("data length %d, but must be even", ln)
	}

	maxSize := 2 * MaxHSaveSize

	if ln <= maxSize {
		return r.client.HSet(context.Background(), key, data).Err()
	}

	startIdx := 0

	for {
		// Будем отправлять не больше maxSize
		blockSize := ln - startIdx
		if blockSize > maxSize {
			blockSize = maxSize
		}

		err := r.client.HSet(context.Background(), key, data[startIdx:startIdx+blockSize]).Err()

		if err != nil {
			return err
		}

		startIdx += blockSize
		if startIdx >= ln {
			return nil
		}
	}
}

// Загрузить все данные из hashtable
func (r *Connection) HLoad(key string) (misc.StringMap, error) {
	res := r.client.HGetAll(context.Background(), key)
	return res.Result()
}

// Загрузить из hashtable данные по списку
func (r *Connection) HLoadSelected(key string, list []string) (misc.StringMap, error) {
	res := r.client.HMGet(context.Background(), key, list...)
	iData, err := res.Result()
	if err != nil {
		return nil, err
	}

	if len(iData) != len(list) {
		return nil, fmt.Errorf("illegal results count %d, expected %d", len(iData), len(list))
	}

	mData := make(misc.StringMap, len(iData))
	for i, v := range iData {
		sv := ""
		if v != nil {
			sv, _ = v.(string)
		}
		mData[list[i]] = sv
	}

	return mData, nil
}

// Длина hashtable
func (r *Connection) HLen(key string) (int, error) {
	v := r.client.HLen(context.Background(), key)
	return int(v.Val()), v.Err()
}

// Удалить из hashtable данные по списку
func (r *Connection) HDel(key string, list []string) error {
	return r.client.HDel(context.Background(), key, list...).Err()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Длина очереди (списка)
func (r *Connection) QLen(key string) (int, error) {
	v := r.client.LLen(context.Background(), key)
	return int(v.Val()), v.Err()
}

// Добавить в конец очереди
// BDRV-2440: Максимальный размер строки в data 536870912 (2**29, полгига)
func (r *Connection) QPush(key string, data interface{}) error {
	return r.client.RPush(context.Background(), key, data).Err()
}

// Получить из начала очереди с удалением
func (r *Connection) QPop(key string) (interface{}, error) {
	v := r.client.LPop(context.Background(), key)
	return v.Val(), v.Err()
}

// Получить из очереди с таймаутом
func (r *Connection) QPopWithTimeout(timeout time.Duration, key ...string) (interface{}, error) {
	v := r.client.BLPop(context.Background(), timeout, key...)
	return v.Val(), v.Err()
}

// Получить из начала очереди без уделения
func (r *Connection) QRead(key string) (interface{}, error) {
	v := r.client.LIndex(context.Background(), key, 0)
	return v.Val(), v.Err()
}

//----------------------------------------------------------------------------------------------------------------------------//

// Сохранить в сортированный список
func (r *Connection) ZSave(key string, members ZMembers) error {
	maxSize := 512*1024 - 1
	ln := len(members)

	for idx := 0; idx < ln; idx += maxSize {
		n := maxSize
		if idx+n > ln {
			n = ln - idx
		}
		err := r.client.ZAdd(context.Background(), key, members[idx:idx+n]...).Err()
		if err != nil {
			return err
		}
	}

	return nil
}

// Получить из сортированного списока по указанным параметрам
func (r *Connection) ZRangeByScore(key string, min int64, max int64, offset int64, count int64) ([]string, error) {
	opt := &redis.ZRangeBy{
		Min:    strconv.FormatInt(min, 10),
		Max:    strconv.FormatInt(max, 10),
		Offset: offset,
		Count:  count,
	}
	v := r.client.ZRangeByScore(context.Background(), key, opt)

	return v.Val(), v.Err()
}

// Получить из сортированного списока по шаблону
func (r *Connection) ZScan(key string, match string, count int64) ([]string, error) {
	data := []string{}
	cursor := uint64(0)

	for {
		v := r.client.ZScan(context.Background(), key, cursor, match, count)

		err := v.Err()
		if err != nil {
			return nil, err
		}

		list, c, err := v.Result()
		if err != nil {
			return nil, err
		}

		if len(list) > 0 {
			data = append(data, list...)
		}

		if c == 0 {
			break
		}

		cursor = c
	}

	return data, nil
}

//----------------------------------------------------------------------------------------------------------------------------//

// Удалить таблицу (ключ)
func (r *Connection) DeleteTable(key string) error {
	return r.client.Del(context.Background(), key).Err()
}

//----------------------------------------------------------------------------------------------------------------------------//
