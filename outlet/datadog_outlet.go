// The outlet pkg is responsible for taking
// buckets from the reader, formatting them in the proper format
// and delivering the formatted datadog metrics to the metric API.
package outlet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"runtime"
	"time"

	"github.com/DataDog/l2met/auth"
	"github.com/DataDog/l2met/bucket"
	"github.com/DataDog/l2met/conf"
	"github.com/DataDog/l2met/metchan"
	"github.com/DataDog/l2met/metrics"
	"github.com/DataDog/l2met/reader"
)

var datadogUrl = "https://app.datadoghq.com/api/v1/series"

type DataDogOutlet struct {
	inbox       chan *bucket.Bucket
	conversions chan *metrics.DataDogMetric
	outbox      chan []*metrics.DataDogMetric
	numOutlets  int
	rdr         *reader.Reader
	conn        *http.Client
	numRetries  int
	Mchan       *metchan.Channel
}

func buildDataDogClient(ttl time.Duration) *http.Client {
	tr := &http.Transport{
		DisableKeepAlives: false,
		Dial: func(n, a string) (net.Conn, error) {
			c, err := net.DialTimeout(n, a, ttl)
			if err != nil {
				return c, err
			}
			return c, c.SetDeadline(time.Now().Add(ttl))
		},
	}
	return &http.Client{Transport: tr}
}

func NewDataDogOutlet(cfg *conf.D, r *reader.Reader) *DataDogOutlet {
	l := &DataDogOutlet{
		conn:        buildDataDogClient(cfg.OutletTtl),
		inbox:       make(chan *bucket.Bucket, cfg.BufferSize),
		conversions: make(chan *metrics.DataDogMetric, cfg.BufferSize),
		outbox:      make(chan []*metrics.DataDogMetric, cfg.BufferSize),
		numOutlets:  cfg.Concurrency,
		numRetries:  cfg.OutletRetries,
		rdr:         r,
	}
	return l
}

func (l *DataDogOutlet) Start() {
	go l.rdr.Start(l.inbox)
	// Converting is CPU bound as it reads from memory
	// then computes statistical functions over an array.
	for i := 0; i < runtime.NumCPU(); i++ {
		go l.convert()
	}
	go l.groupByUser()
	for i := 0; i < l.numOutlets; i++ {
		go l.outlet()
	}
	go l.Report()
}

func (l *DataDogOutlet) convert() {
	for bucket := range l.inbox {
		for _, metric := range bucket.Metrics() {
			for _, m := range metrics.DataDogConvertMetric(metric) {
				l.conversions <- m
			}
		}
		delay := bucket.Id.Delay(time.Now())
		l.Mchan.Measure("outlet.delay", float64(delay))
	}
}

func (l *DataDogOutlet) groupByUser() {
	ticker := time.Tick(time.Millisecond * 200)
	m := make(map[string][]*metrics.DataDogMetric)
	for {
		select {
		case <-ticker:
			for k, v := range m {
				if len(v) > 0 {
					l.outbox <- v
				}
				delete(m, k)
			}
		case payload := <-l.conversions:
			usr := payload.Auth
			if _, present := m[usr]; !present {
				m[usr] = make([]*metrics.DataDogMetric, 1, 300)
				m[usr][0] = payload
			} else {
				m[usr] = append(m[usr], payload)
			}
			if len(m[usr]) == cap(m[usr]) {
				l.outbox <- m[usr]
				delete(m, usr)
			}
		}
	}
}

func (l *DataDogOutlet) outlet() {
	for payloads := range l.outbox {
		if len(payloads) < 1 {
			fmt.Printf("at=%q\n", "empty-metrics-error")
			continue
		}
		//Since a playload contains all metrics for
		//a unique datadog api_key, we can extract the user/pass
		//from any one of the payloads.
		api_key, err := auth.Decrypt(payloads[0].Auth)
		if err != nil {
			fmt.Printf("error=%s\n", err)
			continue
		}
		ddReq := &metrics.DataDogRequest{payloads}

		j, err := json.Marshal(ddReq)
		if err != nil {
			fmt.Printf("at=json error=%s key=%s\n", err, api_key)
			continue
		}
		if err := l.postWithRetry(api_key, j); err != nil {
			l.Mchan.Measure("outlet.drop", 1)
		}
	}
}

func (l *DataDogOutlet) postWithRetry(api_key string, body []byte) error {
	for i := 0; i <= l.numRetries; i++ {
		if err := l.post(api_key, body); err != nil {
			fmt.Printf("measure.datadog.error key=%s msg=%s attempt=%d\n", api_key, err, i)
			if i == l.numRetries {
				return err
			}
			continue
		}
		return nil
	}
	//Should not be possible.
	return errors.New("Unable to post.")
}

func (l *DataDogOutlet) post(api_key string, body []byte) error {
	defer l.Mchan.Time("outlet.post", time.Now())
	b := bytes.NewBuffer(body)
	req, err := http.NewRequest("POST", datadogUrl+"?api_key="+api_key, b)
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("User-Agent", "l2met/"+conf.Version)
	req.Header.Add("Connection", "Keep-Alive")
	resp, err := l.conn.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var m string
		s, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			m = fmt.Sprintf("error=failed-request code=%d", resp.StatusCode)
		} else {
			m = fmt.Sprintf("error=failed-request code=%d resp=body=%s req-body=%s",
				resp.StatusCode, s, body)
		}
		return errors.New(m)
	}
	return nil
}

// Keep an eye on the lenghts of our buffers.
// If they are maxed out, something is going wrong.
func (l *DataDogOutlet) Report() {
	for _ = range time.Tick(time.Second) {
		pre := "datadog-outlet."
		l.Mchan.Measure(pre+"inbox", float64(len(l.inbox)))
		l.Mchan.Measure(pre+"conversion", float64(len(l.conversions)))
		l.Mchan.Measure(pre+"outbox", float64(len(l.outbox)))
	}
}
