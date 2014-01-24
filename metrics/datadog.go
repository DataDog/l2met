package metrics

import (
	"github.com/DataDog/l2met/bucket"
)

type DataDogRequest struct {
	Series []*DataDogMetric `json:"series"`
}

type point [2]float64

type DataDogMetric struct {
	Metric string   `json:"metric"`
	Host   string   `json:"host,omitempty"`
	Tags   []string `json:"tags"`
	Type   string   `json:"type"`
	Auth   string   `json:"-"`
	Points []point  `json:"points"`
}

// Create a datadog metric for a metric and the requested metric type
func DataDogComplexMetric(m *bucket.Metric, mtype string) *DataDogMetric {
	d := &DataDogMetric{
		Type: "gauge",
		Auth: m.Auth,
	}
	switch mtype {
	case "min":
		d.Metric = m.Name + ".min"
		d.Points = []point{{float64(m.Time), *m.Min}}
	case "max":
		d.Metric = m.Name + ".max"
		d.Points = []point{{float64(m.Time), *m.Max}}
	case "sum":
		// XXX: decided that sum would be the 'default' metric name; is this right?
		d.Metric = m.Name
		d.Points = []point{{float64(m.Time), *m.Sum}}
	case "count":
		// FIXME: "counts as counts"?
		d.Metric = m.Name + ".count"
		d.Points = []point{{float64(m.Time), float64(*m.Count)}}
	}
	return d
}

// Convert a metric into one or more datadog metrics.  Metrics marked as
// complex actually map to 4 datadog metrics as there's no "complex" type
// in the datadog API.
func DataDogConvertMetric(m *bucket.Metric) []*DataDogMetric {
	var metrics []*DataDogMetric
	if m.IsComplex {
		metrics = make([]*DataDogMetric, 0, 4)
		metrics = append(metrics, DataDogComplexMetric(m, "min"))
		metrics = append(metrics, DataDogComplexMetric(m, "max"))
		metrics = append(metrics, DataDogComplexMetric(m, "sum"))
		metrics = append(metrics, DataDogComplexMetric(m, "count"))
	} else {
		d := &DataDogMetric{
			Metric: m.Name,
			Type:   "gauge",
			Auth:   m.Auth,
			Points: []point{{float64(m.Time), *m.Val}},
		}
		metrics = []*DataDogMetric{d}
	}
	return metrics
}
