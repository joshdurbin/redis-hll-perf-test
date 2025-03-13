package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/brianvoe/gofakeit/v7"
	"github.com/redis/go-redis/v9"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"gonum.org/v1/gonum/stat"
	"gonum.org/v1/gonum/stat/distuv"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"
)

// Redis Lua scripts
var insertScript = redis.NewScript(`
    local id = ARGV[1] 
    local url = ARGV[2]
    local timestamp = redis.call('TIME')

    redis.call('SADD', 'stats-retrieval_stores', id)
    redis.call('SET', 'stats-retrieval_first_seen-' .. id, timestamp[1], 'NX')
    redis.call('PFADD', 'stats-retrieval_cardinality-' .. id, url)
    redis.call('INCR', 'stats-retrieval_total_seen-' .. id)
    redis.call('SET', 'stats-retrieval_last_seen-' .. id, timestamp[1])

    return "OK"
`)

var readScript = redis.NewScript(`
    local id = ARGV[1]

    local first_seen = redis.call('GET', 'stats-retrieval_first_seen-' .. id) or "0"
    local last_seen = redis.call('GET', 'stats-retrieval_last_seen-' .. id) or "0"
    local cardinality = redis.call('PFCOUNT', 'stats-retrieval_cardinality-' .. id) or 0
    local total_seen = redis.call('GET', 'stats-retrieval_total_seen-' .. id) or "0"

    return { first_seen, last_seen, tostring(cardinality), total_seen }
`)

var deleteScript = redis.NewScript(`
    local ids = redis.call('SMEMBERS', 'stats-retrieval_stores')

    for i, id in ipairs(ids) do
        local first_seen_delete = redis.call('DEL', 'stats-retrieval_first_seen-' .. id) or 0
        local last_seen_delete = redis.call('DEL', 'stats-retrieval_last_seen-' .. id) or 0
        local cardinality_delete = redis.call('DEL', 'stats-retrieval_cardinality-' .. id) or 0    
        local total_seen_delete = redis.call('DEL', 'stats-retrieval_total_seen-' .. id) or 0
        local remove_from_set = redis.call('SREM', 'stats-retrieval_stores', id) or 0
    end

    return "OK"
`)

var (
	// Root command
	rootCmd = &cobra.Command{
		Use:   "redis-benchmark",
		Short: "Redis benchmark tool",
		Long: `Tests insertion of random URLs as well as some other stats into various HLL keys in Redis as a Poc`,
	}

	// Redis client instance
	redisClient *redis.Client
)

func init() {
	// Set up persistent flags that apply to multiple commands
	rootCmd.PersistentFlags().String("redis-addr", ":6379", "Redis server address")
	rootCmd.PersistentFlags().Bool("redis-use-sentinel", false, "Use redis sentinel client using --redis-sentinel-name")
	rootCmd.PersistentFlags().String("redis-sentinel-name", "", "The redis sentinel name to use")
	rootCmd.PersistentFlags().Bool("use-lua", false, "Use Lua scripting instead of Redis transactions")
	viper.BindPFlag("redis-addr", rootCmd.PersistentFlags().Lookup("redis-addr"))
	viper.BindPFlag("redis-use-sentinel", rootCmd.PersistentFlags().Lookup("redis-use-sentinel"))
	viper.BindPFlag("redis-sentinel-name", rootCmd.PersistentFlags().Lookup("redis-sentinel-name"))
	viper.BindPFlag("use-lua", rootCmd.PersistentFlags().Lookup("use-lua"))

	// Create command
	createCmd := &cobra.Command{
		Use:   "create",
		Short: "Create mock traffic activity and store in Redis HLL",
		RunE:  runCreate,
	}
	createCmd.Flags().Int("num-routines", 6, "Number of Routines")
	createCmd.Flags().Int("num-ids", 120000, "The number of ids to use for generation")
	createCmd.Flags().Duration("ticker-duration", 1*time.Second, "The tick interval for stats output during insertion operations")
	createCmd.Flags().Int("concurrency-limit", 1000, "Limits concurrent operations against to the limit")

	// List command
	listCmd := &cobra.Command{
		Use:   "list",
		Short: "List mock traffic activity in Redis HLL",
		RunE:  runList,
	}

	// Delete command
	deleteCmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete all mock traffic activity in Redis",
		RunE:  runDelete,
	}

	// Bind flags to viper
	viper.BindPFlag("create.num-routines", createCmd.Flags().Lookup("num-routines"))
	viper.BindPFlag("create.num-ids", createCmd.Flags().Lookup("num-ids"))
	viper.BindPFlag("create.ticker-duration", createCmd.Flags().Lookup("ticker-duration"))
	viper.BindPFlag("create.concurrency-limit", createCmd.Flags().Lookup("concurrency-limit"))

	// Add commands to root
	rootCmd.AddCommand(createCmd)
	rootCmd.AddCommand(listCmd)
	rootCmd.AddCommand(deleteCmd)

	// Initialize viper settings
	cobra.OnInitialize(initConfig)
}

func initConfig() {
	viper.SetEnvPrefix("HLL_BENCHMARK")
	viper.AutomaticEnv()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func getRedisClient() *redis.Client {
	if redisClient == nil {
		if viper.GetBool("redis-use-sentinel") {
			redisClient = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    viper.GetString("redis-sentinel-name"),
				SentinelAddrs: []string{viper.GetString("redis.addr")}})
		} else {
			redisClient = redis.NewClient(&redis.Options{
				Addr: viper.GetString("redis-addr"),
			})
		}
	}
	return redisClient
}

func runCreate(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Get configuration from viper
	useLua := viper.GetBool("use-lua")
	numRoutines := viper.GetInt("create.num-routines")
	numIds := viper.GetInt("create.num-ids")
	tickerDuration := viper.GetDuration("create.ticker-duration")
	concurrencyLimit := viper.GetInt("create.concurrency-limit")

	// Get Redis client
	redisClient := getRedisClient()
	defer redisClient.Close()

	timeReportingChan := make(chan time.Duration, 1000)

	// Create an errgroup to manage goroutines
	errGroup, errGroupCtx := errgroup.WithContext(ctx)

	limiter := rate.NewLimiter(rate.Limit(concurrencyLimit), concurrencyLimit)

	// Start stats reporter goroutine
	errGroup.Go(func() error {
		ticker := time.NewTicker(tickerDuration)
		defer ticker.Stop()

		totalCount := 0
		tickCount := 0
		timings := []float64{}
		for {
			select {
			case timeSpent, ok := <-timeReportingChan:
				if !ok {
					return nil
				}

				// Increment values when a value comes in from the channel
				totalCount++
				tickCount++
				timings = append(timings, float64(timeSpent.Milliseconds()))
			case <-ticker.C:
				// Sort the timings and only calculate stats if the slice is >0
				sort.Float64s(timings)
				if len(timings) == 0 {
					println(fmt.Sprintf("[%v] no entries processed!", time.Now().Format(time.RFC3339Nano)))
				} else {
					println(fmt.Sprintf("[%v] processed %v entries in %v and %v in total; mean: %.2f ms, p25: %.2f ms, p50: %.2f ms, p75: %.2f ms, p99: %.2f ms",
						time.Now().Format(time.RFC3339Nano),
						tickCount,
						tickerDuration,
						totalCount,
						stat.Mean(timings, nil),
						stat.Quantile(0.25, stat.Empirical, timings, nil),
						stat.Quantile(0.50, stat.Empirical, timings, nil),
						stat.Quantile(0.75, stat.Empirical, timings, nil),
						stat.Quantile(0.99, stat.Empirical, timings, nil)))

					// Reset window values
					tickCount = 0
					timings = []float64{}
				}
			case <-ctx.Done():
				return nil
			}
		}
	})

	// Generate ids to be used by the goroutines below
	var probabilityMap sync.Map

	// Ensure a normal distribution of values
	normal := distuv.Normal{
		Mu:    0, // Mean (standard normal distribution)
		Sigma: 1, // Standard deviation
		Src:   rand.NewSource(uint64(time.Now().UnixNano())),
	}

	// Place the values in the concurrent map
	for i := 1; i <= numIds; i++ {
		probabilityMap.Store(i, normal.Rand())
	}

	// Spin up insertion routines
	for i := 0; i < numRoutines; i++ {
		errGroup.Go(func() error {
			return createData(errGroupCtx, redisClient, &probabilityMap, useLua, numIds, timeReportingChan, limiter)
		})
	}

	// Wait
	return errGroup.Wait()
}

func runList(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Get configuration from viper
	useLua := viper.GetBool("use-lua")

	// Get Redis client
	redisClient := getRedisClient()
	defer redisClient.Close()

	return listData(ctx, redisClient, useLua)
}

func runDelete(cmd *cobra.Command, args []string) error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Get Redis client
	redisClient := getRedisClient()
	defer redisClient.Close()

	_, err := deleteScript.Run(ctx, redisClient, []string{""}).Result()
	return err
}

func createData(ctx context.Context, redisClient *redis.Client, probabilityMap *sync.Map, useLua bool, numIds int, timeReportingChan chan<- time.Duration, limiter *rate.Limiter) error {
	src := rand.NewSource(uint64(time.Now().UnixNano()))
	r := rand.New(src)

	// Insert data, loop forever until cancellation occurs which exits this func as a returned error from the context
	// aware redis operations
	for {
		// This is MUCH faster than generating on a loop adjusting a loop counter
		id := r.Intn(numIds) + 1

		probabilityOfInsert, ok := probabilityMap.Load(id)
		if ok {
			// Simulate randomness
			if r.Float64() < probabilityOfInsert.(float64) {
				err := limiter.Wait(ctx)
				if err != nil {
					return err
				}

				// Execute calls within a transaction as each individual operation around trip on the same host is ~1 ms
				starTime := time.Now()

				if useLua {
					_, err := insertScript.Run(ctx, redisClient, []string{""}, id, gofakeit.URL()).Result()
					if err != nil {
						return err
					}
				} else {
					// Create transaction pipeline
					redisClienttx := redisClient.TxPipeline()

					// Add the id to a set so we can easily rebuild this data for retrieval
					_, err := redisClienttx.SAdd(ctx, "stats-retrieval_stores", id).Result()
					if err != nil {
						return err
					}

					// Set the first seen with set nx (do not set if exists already)
					_, err = redisClienttx.SetNX(ctx, fmt.Sprintf("stats-retrieval_first_seen-%v", id), starTime.Unix(), 0).Result()
					if err != nil {
						return err
					}

					// Add the uri to the hyperloglog
					_, err = redisClienttx.PFAdd(ctx, fmt.Sprintf("stats-retrieval_cardinality-%v", id), gofakeit.URL()).Result()
					if err != nil {
						return err
					}

					// Increment the total operations seen
					_, err = redisClienttx.Incr(ctx, fmt.Sprintf("stats-retrieval_total_seen-%v", id)).Result()
					if err != nil {
						return err
					}

					// Set the last time the operation was seen
					_, err = redisClienttx.Set(ctx, fmt.Sprintf("stats-retrieval_last_seen-%v", id), starTime.Unix(), 0).Result()
					if err != nil {
						return err
					}

					// Execute transaction
					_, err = redisClienttx.Exec(ctx)
					if err != nil {
						return err
					}
				}

				timeReportingChan <- time.Since(starTime)
			}
		}
	}
}

func listData(ctx context.Context, redisClient *redis.Client, useLua bool) error {
	ids, err := redisClient.SMembers(ctx, "stats-retrieval_stores").Result()
	if err != nil {
		return err
	}

	// Create a new tabwriter that writes to stdout
	w := tabwriter.NewWriter(os.Stdout, 10, 0, 2, ' ', tabwriter.Debug)

	// Print header
	fmt.Fprintln(w, "ID\tFirst Seen\tLast Seen\tCardinality\tTotal Seen\t")

	// Print separator
	fmt.Fprintln(w, "----\t----\t----\t----\t----\t")

	for _, id := range ids {
		if ctx.Err() != nil {
			return err
		}

		var output string

		if useLua {
			result, err := readScript.Run(ctx, redisClient, []string{""}, id).StringSlice()
			if err != nil {
				return err
			}

			if len(result) != 4 {
				return errors.New("return ")
			}

			// Convert first seen
			firstSeenInt, err := strconv.ParseInt(result[0], 10, 64)
			if err != nil {
				return err
			}
			firstSeenUnixTime := time.Unix(firstSeenInt, 0)

			// Convert last seen
			lastSeenInt, err := strconv.ParseInt(result[1], 10, 64)
			if err != nil {
				return err
			}
			lastSeenUnixTime := time.Unix(lastSeenInt, 0)

			output = fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t\n", id, firstSeenUnixTime.Format(time.RFC3339), lastSeenUnixTime.Format(time.RFC3339), result[2], result[3])
		} else {
			pipe := redisClient.TxPipeline()

			firstSeenCmd := pipe.Get(ctx, fmt.Sprintf("stats-retrieval_first_seen-%v", id))
			cardinalityCountCmd := pipe.PFCount(ctx, fmt.Sprintf("stats-retrieval_cardinality-%v", id))
			totalSeenCmd := pipe.Get(ctx, fmt.Sprintf("stats-retrieval_total_seen-%v", id))
			lastSeenCmd := pipe.Get(ctx, fmt.Sprintf("stats-retrieval_last_seen-%v", id))

			// Execute the transaction
			_, err := pipe.Exec(ctx)
			if err != nil {
				return err
			}

			firstSeen, err := firstSeenCmd.Result()
			if err != nil {
				return err
			}
			firstSeenInt, err := strconv.ParseInt(firstSeen, 10, 64)
			if err != nil {
				return err
			}
			firstSeenUnixTime := time.Unix(firstSeenInt, 0)

			cardinalityCount, err := cardinalityCountCmd.Result()
			if err != nil {
				return err
			}
			totalSeen, err := totalSeenCmd.Result()
			if err != nil {
				return err
			}

			lastSeen, err := lastSeenCmd.Result()
			if err != nil {
				return err
			}
			lastSeenInt, err := strconv.ParseInt(lastSeen, 10, 64)
			if err != nil {
				return err
			}
			lastSeenUnixTime := time.Unix(lastSeenInt, 0)

			output = fmt.Sprintf("%s\t%s\t%s\t%d\t%s\t\n", id, firstSeenUnixTime.Format(time.RFC3339), lastSeenUnixTime.Format(time.RFC3339), cardinalityCount, totalSeen)
		}

		// write to the writer
		_, err = fmt.Fprintf(w, output)
		if err != nil {
			return err
		}

		// flush to "simulate" streaming
		err = w.Flush()
		if err != nil {
			return err
		}
	}

	return w.Flush()
}
