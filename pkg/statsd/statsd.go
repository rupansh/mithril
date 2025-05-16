package statsd

import (
	"runtime/metrics"
	"strings"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Overclock-Validator/mithril/pkg/mlog"
)

var statsdClient *statsd.Client

func init() {
	var err error
	statsdClient, err = statsd.New("127.0.0.1:8125")
	if err != nil {
		mlog.Log.Errorf("couldn't start statsdClient: %v", err)
	}
	statsdClient.Namespace = "mithril."
	periodicallySendRuntimeMetrics()
}

func Count(name string, value int64, tags []string, rate float64) error {
	if statsdClient == nil {
		return nil
	}
	return statsdClient.Count(name, value, tags, rate)
}

func Distribution(name string, value float64, tags []string, rate float64) error {
	if statsdClient == nil {
		return nil
	}
	return statsdClient.Distribution(name, value, tags, rate)
}

func Gauge(name string, value float64, tags []string, rate float64) error {
	if statsdClient == nil {
		return nil
	}
	return statsdClient.Gauge(name, value, tags, rate)
}

func periodicallySendRuntimeMetrics() {
	descs := metrics.All()
	var samples []metrics.Sample
	for _, desc := range descs {
		if strings.Contains(desc.Name, "/memory/classes") {
			samples = append(samples, metrics.Sample{Name: desc.Name})
		}
	}
	ticker := time.NewTicker(5 * time.Second)

	go func() {
		for {
			select {
			case <-ticker.C:
				metrics.Read(samples)
				for _, sample := range samples {
					metricName := strings.TrimPrefix(strings.Map(func(r rune) rune {
						if r == '/' || r == ':' {
							return '.'
						}
						return r
					}, sample.Name), ".")
					switch sample.Value.Kind() {
					case metrics.KindUint64:
						Gauge(metricName, float64(sample.Value.Uint64()), nil, 1)
					case metrics.KindFloat64:
						Gauge(metricName, float64(sample.Value.Float64()), nil, 1)
					default:
						mlog.Log.Errorf("unknown metric kind: metric=%s kind=%d", sample.Name, sample.Value.Kind())
					}
				}
			}
		}
	}()
}
