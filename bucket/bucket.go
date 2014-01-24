// A collection of measurements.
package bucket

import (
	"fmt"
	"math"
	"sort"
	"sync"
)

type MetricAttrs struct {
	Min   int
	Units string
}

// When submitting data upstream, we need to coerce
// our bucket representation into something their API
// can handle. Because there is not a 1-1 parity
// with the statistical functions that a bucket offers and
// the types of data the other APIs accept (e.g. Librato does-
// not have support for perc50, perc95, perc99) we need to expand
// our bucket into a set of Metric(s).
type Metric struct {
	Name      string
	Time      int64
	Val       *float64
	Count     *int
	Sum       *float64
	Max       *float64
	Min       *float64
	Source    string
	Auth      string
	Attr      *MetricAttrs
	IsComplex bool
}

type Bucket struct {
	sync.Mutex
	Id   *Id
	Vals []float64
	Sum  float64
}

func (b *Bucket) Reset() {
	b.Lock()
	defer b.Unlock()
	b.Sum = 0
	b.Vals = b.Vals[:0]
}

func (b *Bucket) Append(val float64) {
	b.Lock()
	defer b.Unlock()
	b.Sum += val
	b.Vals = append(b.Vals, val)
}

func (b *Bucket) Incr(val float64) {
	b.Lock()
	defer b.Unlock()
	b.Sum += val
}

func (b *Bucket) Merge(other *Bucket) {
	other.Lock()
	defer other.Unlock()
	for _, v := range other.Vals {
		b.Append(v)
	}
}

// Relies on the Emitter to determine which type of
// metrics should be returned.
func (b *Bucket) Metrics() []*Metric {
	switch b.Id.Type {
	case "measurement":
		return b.EmitMeasurements()
	case "counter":
		return b.EmitCounters()
	case "sample":
		return b.EmitSamples()
	default:
		panic("Undefined bucket.Id type.")
	}
}

// The standard emitter. All log data with `measure.foo` will
// be mapped to the MeasureEmitter.
func (b *Bucket) EmitMeasurements() []*Metric {
	metrics := make([]*Metric, 4)
	metrics[0] = b.ComplexMetric()
	metrics[1] = b.Metric(".median", b.Median())
	metrics[2] = b.Metric(".perc95", b.Perc95())
	metrics[3] = b.Metric(".perc99", b.Perc99())
	return metrics
}

func (b *Bucket) EmitCounters() []*Metric {
	metrics := make([]*Metric, 1)
	metrics[0] = b.Metric("", b.Sum)
	return metrics
}

func (b *Bucket) EmitSamples() []*Metric {
	metrics := make([]*Metric, 1)
	metrics[0] = b.Metric("", b.Last())
	return metrics
}

func (b *Bucket) ComplexMetric() *Metric {
	min := b.Min()
	max := b.Max()
	cnt := b.Count()
	sum := b.Sum
	return &Metric{
		Attr: &MetricAttrs{
			Min:   0,
			Units: b.Id.Units,
		},
		Name:      b.Id.Name,
		Source:    b.Id.Source,
		Time:      b.Id.Time.Unix(),
		Auth:      b.Id.Auth,
		Min:       &min,
		Max:       &max,
		Sum:       &sum,
		Count:     &cnt,
		IsComplex: true,
	}
}

// If an non-empty suffix is given, the name of the resulting Metric
// will contain the suffix.
func (b *Bucket) Metric(suffix string, val float64) *Metric {
	return &Metric{
		Attr: &MetricAttrs{
			Min:   0,
			Units: b.Id.Units,
		},
		Name:   b.Id.Name + suffix,
		Source: b.Id.Source,
		Time:   b.Id.Time.Unix(),
		Auth:   b.Id.Auth,
		Val:    &val,
	}
}

func (b *Bucket) String() string {
	return fmt.Sprintf("name=%s source=%s vals=%v",
		b.Id.Name, b.Id.Source, b.Vals)
}

func (b *Bucket) Count() int {
	return len(b.Vals)
}

func (b *Bucket) Mean() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	return b.Sum / float64(b.Count())
}

func (b *Bucket) Sort() {
	if !sort.Float64sAreSorted(b.Vals) {
		sort.Float64s(b.Vals)
	}
}

func (b *Bucket) Min() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	return b.Vals[0]
}

func (b *Bucket) Median() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Ceil(float64(b.Count() / 2)))
	return b.Vals[pos]
}

func (b *Bucket) Perc95() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.95))
	return b.Vals[pos]
}

func (b *Bucket) Perc99() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := int(math.Floor(float64(b.Count()) * 0.99))
	return b.Vals[pos]
}

func (b *Bucket) Max() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	b.Sort()
	pos := b.Count() - 1
	return b.Vals[pos]
}

func (b *Bucket) Last() float64 {
	if b.Count() == 0 {
		return float64(0)
	}
	pos := b.Count() - 1
	return b.Vals[pos]
}
