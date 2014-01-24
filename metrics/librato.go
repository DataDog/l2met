package metrics

import "github.com/DataDog/l2met/bucket"

type LibratoRequest struct {
	Gauges []*LibratoMetric `json:"gauges"`
}

type LibratoAttrs struct {
	Min   int    `json:"display_min"`
	Units string `json:"display_units_long"`
}

// When submitting data to Librato, we need to coerce
// our bucket representation into something their API
// can handle. Because there is not a 1-1 parity
// with the statistical functions that a bucket offers and
// the types of data the Librato API accepts (e.g. Librato does-
// not have support for perc50, perc95, perc99) we need to expand
// our bucket into a set of LibratoMetric(s).
type LibratoMetric struct {
	Name   string        `json:"name"`
	Time   int64         `json:"measure_time"`
	Val    *float64      `json:"value,omitempty"`
	Count  *int          `json:"count,omitempty"`
	Sum    *float64      `json:"sum,omitempty"`
	Max    *float64      `json:"max,omitempty"`
	Min    *float64      `json:"min,omitempty"`
	Source string        `json:"source,omitempty"`
	Auth   string        `json:"-"`
	Attr   *LibratoAttrs `json:"attributes,omitempty"`
}

// Convert a bucket.Metric to a LibratoMetric
func LibratoConvertMetric(m *bucket.Metric) *LibratoMetric {
	attrs := &LibratoAttrs{
		Min:   m.Attr.Min,
		Units: m.Attr.Units,
	}
	l := &LibratoMetric{
		Name:   m.Name,
		Time:   m.Time,
		Val:    m.Val,
		Count:  m.Count,
		Sum:    m.Sum,
		Max:    m.Max,
		Min:    m.Min,
		Source: m.Source,
		Auth:   m.Auth,
		Attr:   attrs,
	}
	return l

}
