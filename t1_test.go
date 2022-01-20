package redis

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/alrusov/config"
	"github.com/go-redis/redis/v8"
)

//----------------------------------------------------------------------------------------------------------------------------//

var (
	testConn = &Config{
		Host:     "localhost:6379",
		Password: "",
		DB:       4,
		Timeout:  10 * config.Duration(time.Second),
	}
)

//----------------------------------------------------------------------------------------------------------------------------//

func TestLoadSelected(t *testing.T) {
	conn := New(testConn)

	list := []string{"TestComm_Tag_4", "TestComm_Tag_1", "TestComm_Tag_2", "TestComm_Tag_3", "TestComm_Tag_NOtag1", "TestComm_Tag_NOtag2", "TestComm_Tag_5"}
	data, err := conn.HLoadSelected("archive_last", list)
	if err != nil {
		t.Errorf("%s", err)
		return
	}
	t.Log(data)
}

//----------------------------------------------------------------------------------------------------------------------------//

func TestHSave(t *testing.T) {
	testHSave(t, 0)
	testHSave(t, 1)
	testHSave(t, 2)
	testHSave(t, 5)
	testHSave(t, MaxHSaveSize-2)
	testHSave(t, MaxHSaveSize-1)
	testHSave(t, MaxHSaveSize)
	testHSave(t, MaxHSaveSize+1)
	testHSave(t, MaxHSaveSize*3+100)
}

func testHSave(t *testing.T, count int) {
	key := "___redis.TestHSave___"

	conn := New(testConn)

	conn.DeleteTable(key)
	defer conn.DeleteTable(key)

	data := make([]string, count*2)

	for i := 0; i < count*2; i += 2 {
		v := strconv.Itoa(i / 2)
		data[i] = v
		data[i+1] = v
	}

	err := conn.HSave(key, data)
	if err != nil {
		t.Fatal(err)
	}

	result, err := conn.HLoad(key)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != count {
		err = fmt.Errorf("found %d records, %d expected", len(result), count)
		t.Fatal(err)
	}

	exists := make([]bool, count)
	for n, v := range result {
		if n != v {
			err = fmt.Errorf("%s != %s", n, v)
			t.Fatal(err)
		}

		i, err := strconv.ParseInt(n, 10, 64)
		if err != nil {
			t.Fatal(err)
		}

		if i < 0 || i > int64(count) {
			err = fmt.Errorf("Illegal value %d not in range 0..%d", i, count)
			t.Fatal(err)
		}
		exists[i] = true
	}

	for i, v := range exists {
		if !v {
			err = fmt.Errorf("%d not found", i)
			t.Fatal(err)
		}
	}

	conn.DeleteTable(key)
}

//----------------------------------------------------------------------------------------------------------------------------//

func BenchmarkHSaveSimple(b *testing.B) {
	key := "___redis.BenchmarkHSaveSimple___"

	conn := New(testConn)

	n := b.N

	keys := make([]string, n)
	for i := 0; i < n; i++ {
		keys[i] = "calc_" + strconv.Itoa(i)
	}

	data := []string{"v", "xxx"}

	var err error

	b.ResetTimer()
	for i := 0; i < n; i++ {
		err = conn.HSave(keys[i], data)
	}
	b.StopTimer()

	conn.DeleteTable(key)

	if err != nil {
		b.Fatal(err)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func BenchmarkHSave(b *testing.B) {
	key := "___redis.BenchmarkHSave___"

	conn := New(testConn)

	n := b.N

	data := make([]string, n*2)

	for i := 0; i < n*2; i += 2 {
		id := strconv.Itoa(i)
		data[i] = id
		data[i+1] = "xxx" + id
	}

	var err error

	b.ResetTimer()
	err = conn.HSave(key, data)
	b.StopTimer()

	conn.DeleteTable(key)

	if err != nil {
		b.Fatal(err)
	}
}

//----------------------------------------------------------------------------------------------------------------------------//

func BenchmarkZSave(b *testing.B) {
	key := "___redis.BenchmarkZSave___"

	b.ResetTimer()
	conn, err := testZSave(b, key, true)
	b.StopTimer()

	conn.DeleteTable(key)

	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkZRangeByScore(b *testing.B) {
	key := "___redis.ZRangeByScore___"

	conn, err := testZSave(b, key, false)
	if err != nil {
		b.Fatal(err)
		conn.DeleteTable(key)
		return
	}

	b.ResetTimer()
	_, err = conn.ZRangeByScore(key, 0, int64(b.N), 0, int64(b.N))
	b.StopTimer()

	conn.DeleteTable(key)

	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkZScan(b *testing.B) {
	key := "___redis.ZScan___"

	conn, err := testZSave(b, key, false)
	if err != nil {
		b.Fatal(err)
		conn.DeleteTable(key)
		return
	}

	b.ResetTimer()
	_, err = conn.ZScan(key, `*"status":192,*`, int64(b.N))
	b.StopTimer()

	conn.DeleteTable(key)

	if err != nil {
		b.Fatal(err)
	}

	/*
		v1 := ""
		v2 := ""
		v3 := ""
		v4 := ""
		if len(x) > 0 {
			v1 = x[0]
			v2 = x[1]
			v3 = x[len(x)-2]
			v4 = x[len(x)-1]
		}
		fmt.Printf("\n%d\n%s %s\n%s %s\n", len(x), v1, v2, v3, v4)
	*/
}

func testZSave(b *testing.B, key string, withReset bool) (*Connection, error) {
	conn := New(testConn)

	n := b.N
	members := make(ZMembers, n)

	for i := 0; i < n; i++ {
		members[i] = &redis.Z{
			Score: float64(i),
			Member: fmt.Sprintf(`{"id":%d,"name":"CalcMass%d","template":"Calc_Mass","status":%d,"datetime":"2020-06-07T01:02:%dZ","value":%f}`,
				i, i, i%256, i%60, float64(i)*float64(i)),
		}
	}

	if withReset {
		b.ResetTimer()
	}

	err := conn.ZSave(key, members)

	if withReset {
		b.StopTimer()
	}

	if err != nil {
		return nil, err
	}

	return conn, nil
}

//----------------------------------------------------------------------------------------------------------------------------//
