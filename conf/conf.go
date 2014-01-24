// Conf exposes a data structure containing all of the
// l2met configuration data. Combines cmd flags and env vars.
package conf

import (
	"errors"
	"flag"
	"net/url"
	"os"
	"strings"
	"time"
)

type D struct {
	PrintVersion     bool
	AppName          string
	RedisHost        string
	RedisPass        string
	MetchanUrl       *url.URL
	Secrets          []string
	BufferSize       int
	Concurrency      int
	Port             int
	ReceiverDeadline int64
	OutletRetries    int
	OutletTtl        time.Duration
	MaxPartitions    uint64
	FlushInterval    time.Duration
	OutletInterval   time.Duration
	DataDogApiBase   string
	UsingReciever    bool
	UseLibratoOutlet bool
	UseDataDogOutlet bool
	Verbose          bool
}

// Builds a conf data structure and connects
// the fields in the struct to flags.
// It is up to the caller to call flag.Parse()
func New() *D {
	d := new(D)

	flag.BoolVar(&d.PrintVersion, "version", false,
		"Print l2met version and sha.")

	flag.StringVar(&d.AppName, "app-name", "l2met",
		"Prefix internal log messages with this value.")

	flag.IntVar(&d.BufferSize, "buffer", 1024,
		"Max number of items for all internal buffers.")

	flag.IntVar(&d.Concurrency, "concurrency", 10,
		"Number of running go routines for outlet or receiver.")

	flag.IntVar(&d.Port, "port", 8080,
		"HTTP server's bind port.")

	flag.IntVar(&d.OutletRetries, "outlet-retry", 2,
		"Number of attempts to outlet metrics.")

	flag.Int64Var(&d.ReceiverDeadline, "recv-deadline", 2,
		"Number of time units to pass before dropping incoming logs.")

	flag.DurationVar(&d.OutletTtl, "outlet-ttl", time.Second*2,
		"Timeout set on outlet HTTP requests.")

	flag.Uint64Var(&d.MaxPartitions, "partitions", uint64(1),
		"Number of partitions to use for outlets.")

	flag.DurationVar(&d.FlushInterval, "flush-interval", time.Second,
		"Time to wait before sending data to store or outlet. "+
			"Example:60s 30s 1m")

	flag.DurationVar(&d.OutletInterval, "outlet-interval", time.Second,
		"Time to wait before outlets read buckets from the store. "+
			"Example:60s 30s 1m")

	flag.BoolVar(&d.UseDataDogOutlet, "outlet-datadog", false,
		"Start the DataDog outlet.")

	flag.StringVar(&d.DataDogApiBase, "datadog-api-base", "",
		"Base url for the DataDog API.")

	flag.BoolVar(&d.UseLibratoOutlet, "outlet-librato", false,
		"Start the Librato outlet.")

	flag.BoolVar(&d.UsingReciever, "receiver", false,
		"Enable the Receiver.")

	flag.BoolVar(&d.Verbose, "v", false,
		"Enable verbose log output.")

	d.RedisHost, d.RedisPass, _ = parseRedisUrl(env("REDIS_URL"))

	// SECRETS are used to decrypt incoming credentials.
	// You can encrypt the credentials with the secret by hitting /sign, as explained here:
	//   https://github.com/ryandotsmith/l2met/wiki/Usage#encrypted-librato-credentials
	d.Secrets = strings.Split(mustenv("SECRETS"), ":")

	if len(env("METCHAN_URL")) > 0 {
		url, err := url.Parse(env("METCHAN_URL"))
		if err == nil {
			d.MetchanUrl = url
		}
	}

	return d
}

// Helper Function
func env(n string) string {
	return os.Getenv(n)
}

func mustenv(n string) string {
	v := env(n)
	if len(v) == 0 {
		panic("Must set: " + n)
	}
	return v
}

// Helper Function
func parseRedisUrl(s string) (string, string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", "", errors.New("Missing REDIS_URL")
	}
	var password string
	if u.User != nil {
		password, _ = u.User.Password()
	}
	return u.Host, password, nil
}
