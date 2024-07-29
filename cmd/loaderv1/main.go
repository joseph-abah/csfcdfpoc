package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/redis/go-redis/v9"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	batchSize = 1000

	numWorkers = 10

	redisHost = "localhost:6379"
)

func main() {

	fileName := flag.String("file", "", "file to load")
	flush := flag.Bool("flush", false, "flush db")

	flag.Parse()

	if *fileName == "" {
		fmt.Println("Missing file parameter")
		os.Exit(1)
	}

	ctx := context.Background()

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisHost,
	})

	if *flush {
		retryStartTime := time.Now()
		flushDb(ctx, redisClient)
		fmt.Printf("DB flushed in %s\n", time.Since(retryStartTime))

	}

	startsAt := time.Now()

	file, err := os.Open(*fileName)
	if err != nil {
		fmt.Println("Error opening file", err)
		os.Exit(1)
	}
	defer file.Close()

	var wg sync.WaitGroup
	lineChan := make(chan string, batchSize)

	go func() {
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			lineChan <- scanner.Text()
		}
		close(lineChan)
	}()

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			batch := make([]string, 0, batchSize)
			for line := range lineChan {
				batch = append(batch, line)
				if len(batch) == batchSize {
					err := streamToRedis(ctx, redisClient, batch)
					if err != nil {
						fmt.Printf("Error streaming to redis: %s", err)
					}
					batch = batch[:0]
				}
			}
			if len(batch) > 0 {
				err := streamToRedis(ctx, redisClient, batch)
				if err != nil {
					fmt.Printf("Error streaming to redis: %s", err)
				}
			}
		}()

	}

	wg.Wait()

	fmt.Printf("Data loaded in %s\n", time.Since(startsAt))

}

func flushDb(ctx context.Context, client *redis.Client) {
	for i := 0; i < 3; i++ {
		err := client.FlushDB(ctx).Err()
		if err == nil {
			return
		}
		fmt.Printf("Error flushing db: %s: retrying after 1 second\n", err)
		time.Sleep(time.Second)
	}
}

func streamToRedis(ctx context.Context, client *redis.Client, batch []string) error {
	pipe := client.Pipeline()
	for _, line := range batch {
		parts := strings.Split(line, ",")
		if len(parts) < 2 {
			return fmt.Errorf("invalid line: %v\n", parts)
			continue
		}

		key := strings.TrimSpace(clean(parts[0]))
		value := clean(strings.Join(parts[1:], ","))
		pipe.Set(ctx, key, value, 0)
	}
	_, err := pipe.Exec(ctx)
	return err
}
func clean(s string) string {
	return strings.ReplaceAll(s, `"`, "")
}
