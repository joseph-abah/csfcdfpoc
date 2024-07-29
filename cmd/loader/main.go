package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/edsrzf/mmap-go"
	"github.com/redis/go-redis/v9"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	batchSize = 10000

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
	if err := loadData(ctx, redisClient, *fileName); err != nil {
		fmt.Printf("Error loading data: %s", err)
		os.Exit(1)
	}

	fmt.Printf("Data loaded in %s\n", time.Since(startsAt))

}

func loadData(ctx context.Context, client *redis.Client, fileName string) error {

	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	mmapFile, err := mmap.Map(file, mmap.RDONLY, 0)
	if err != nil {
		return err
	}

	defer mmapFile.Unmap()

	var wg sync.WaitGroup
	lineChan := make(chan string, batchSize)
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			process(ctx, lineChan, client)
		}()

	}

	scanner := bufio.NewScanner(strings.NewReader(string(mmapFile)))
	for scanner.Scan() {
		lineChan <- scanner.Text()
	}
	close(lineChan)

	wg.Wait()
	return nil
}

func process(ctx context.Context, lineChan chan string, client *redis.Client) {
	var batch []string
	for line := range lineChan {
		batch = append(batch, line)
		if len(batch) == batchSize {
			err := streamToRedis(ctx, client, batch)
			if err != nil {
				fmt.Printf("Error streaming to redis: %s\n", err)
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		err := streamToRedis(ctx, client, batch)
		if err != nil {
			fmt.Printf("Error streaming to redis: %s\n", err)
		}
	}

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

		key := strings.TrimSpace(strings.ReplaceAll(parts[0], `"`, ""))
		value := strings.ReplaceAll(strings.Join(parts[1:], ","), `"`, "")
		pipe.Set(ctx, key, value, 0)
	}
	_, err := pipe.Exec(ctx)
	return err
}
