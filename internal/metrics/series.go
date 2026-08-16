// Package metrics collects the numbers the Monitoring tab draws.
//
// Everything here comes from the Kafka API: message rates from end-offset
// deltas, byte rates estimated from log-dir growth, consumer lag from committed
// offsets. Anything that needs JMX belongs in jmx.go and is optional per
// cluster; a value that cannot be measured is reported as unknown rather than
// invented.
//
// Like internal/kafka, this package does not import bubbletea. The collector
// runs on its own goroutine and hands out snapshots; internal/ui turns those
// into messages.
package metrics

import (
	"math"
	"sort"
	"time"
)

// Point is one sample.
type Point struct {
	At    time.Time
	Value float64
}

// Series is a fixed-size ring of samples, oldest first once it wraps.
//
// Nothing is written to disk: a monitoring view is worth what it shows now, and
// keeping history would turn a read-only tool into one that owns state.
type Series struct {
	points []Point
	next   int
	full   bool
}

// NewSeries creates a series holding at most capacity samples.
func NewSeries(capacity int) *Series {
	if capacity < 1 {
		capacity = 1
	}
	return &Series{points: make([]Point, capacity)}
}

// Add appends a sample.
func (s *Series) Add(at time.Time, value float64) {
	s.points[s.next] = Point{At: at, Value: value}
	s.next++
	if s.next == len(s.points) {
		s.next, s.full = 0, true
	}
}

// Len is the number of samples held.
func (s *Series) Len() int {
	if s.full {
		return len(s.points)
	}
	return s.next
}

// Cap is the number of samples the series can hold.
func (s *Series) Cap() int { return len(s.points) }

// All returns every sample, oldest first.
func (s *Series) All() []Point {
	out := make([]Point, 0, s.Len())
	if s.full {
		out = append(out, s.points[s.next:]...)
	}
	out = append(out, s.points[:s.next]...)
	return out
}

// Window returns the samples taken within d of the newest one.
//
// The window is measured from the last sample rather than from time.Now, so a
// stalled collector shows its last window instead of an empty chart that would
// read as "nothing is happening".
func (s *Series) Window(d time.Duration) []Point {
	all := s.All()
	if len(all) == 0 || d <= 0 {
		return all
	}

	cutoff := all[len(all)-1].At.Add(-d)
	for i, point := range all {
		if !point.At.Before(cutoff) {
			return all[i:]
		}
	}
	return nil
}

// Values returns just the values of the given points.
func Values(points []Point) []float64 {
	values := make([]float64, len(points))
	for i, point := range points {
		values[i] = point.Value
	}
	return values
}

// Last is the newest sample, and false when the series is empty.
func (s *Series) Last() (Point, bool) {
	if s.Len() == 0 {
		return Point{}, false
	}
	index := s.next - 1
	if index < 0 {
		index = len(s.points) - 1
	}
	return s.points[index], true
}

// LastValue is the newest value, or 0 when there is none.
func (s *Series) LastValue() float64 {
	if point, ok := s.Last(); ok {
		return point.Value
	}
	return 0
}

// Max is the largest value in the window.
func Max(points []Point) float64 {
	max := 0.0
	for _, point := range points {
		if point.Value > max {
			max = point.Value
		}
	}
	return max
}

// Mean is the average value of the points, or 0 for none.
func Mean(points []Point) float64 {
	if len(points) == 0 {
		return 0
	}
	var sum float64
	for _, point := range points {
		sum += point.Value
	}
	return sum / float64(len(points))
}

// Trend is which way a series is going.
type Trend int

// The trends a series can be in.
const (
	TrendIdle Trend = iota
	TrendSteady
	TrendGrowing
	TrendDraining
)

func (t Trend) String() string {
	switch t {
	case TrendGrowing:
		return "growing"
	case TrendDraining:
		return "draining"
	case TrendSteady:
		return "steady"
	default:
		return "idle"
	}
}

// trendThreshold is how much the two halves of a window have to differ before
// the movement is called a trend. Consumer lag is never perfectly flat, and
// calling every wobble "growing" would make the column worthless.
const trendThreshold = 0.1

// TrendOf compares the first half of the window with the second.
//
// Two halves rather than a fitted slope: a lag series is noisy and a slope over
// noise flips sign constantly, while "where it was against where it is" is both
// stable and what someone actually wants to know.
func TrendOf(points []Point) Trend {
	if len(points) < 4 {
		if Max(points) == 0 {
			return TrendIdle
		}
		return TrendSteady
	}

	half := len(points) / 2
	before, after := Mean(points[:half]), Mean(points[half:])

	if before == 0 && after == 0 {
		return TrendIdle
	}

	// The change is measured against the larger half, so going from 10 to 0 and
	// from 0 to 10 are equally significant.
	scale := math.Max(before, after)
	change := (after - before) / scale

	switch {
	case change > trendThreshold:
		return TrendGrowing
	case change < -trendThreshold:
		return TrendDraining
	}

	// A relative threshold alone calls a big lag that keeps climbing "steady":
	// ten percent of a hundred thousand is a lot of records. A series that only
	// ever moves one way is going that way whatever the percentage says, and
	// that is exactly what an operator means by growing.
	return monotonicTrend(points)
}

// monotonicTrend reports the direction of a series that never turns back, and
// steady for anything that does.
func monotonicTrend(points []Point) Trend {
	rising, falling := true, true
	for i := 1; i < len(points); i++ {
		switch {
		case points[i].Value > points[i-1].Value:
			falling = false
		case points[i].Value < points[i-1].Value:
			rising = false
		}
	}

	first, last := points[0].Value, points[len(points)-1].Value
	switch {
	case rising && last > first:
		return TrendGrowing
	case falling && last < first:
		return TrendDraining
	default:
		return TrendSteady
	}
}

// Rate turns a counter reading into a per-second rate.
//
// A counter that goes backwards means the topic was truncated or recreated
// between polls, not that records were unwritten; such a step is reported as
// zero rather than as a negative rate.
func Rate(previous, current int64, elapsed time.Duration) float64 {
	if elapsed <= 0 || current < previous {
		return 0
	}
	return float64(current-previous) / elapsed.Seconds()
}

// RateF is Rate for float64 counters, as read from a Prometheus exposition: a
// counter that goes backwards means the process restarted, and the step is
// reported as zero rather than as a negative rate.
func RateF(previous, current float64, elapsed time.Duration) float64 {
	if elapsed <= 0 || current < previous {
		return 0
	}
	return (current - previous) / elapsed.Seconds()
}

// TopSeries names the series with the largest last values, most first.
func TopSeries(series map[string]*Series, n int) []string {
	names := make([]string, 0, len(series))
	for name := range series {
		names = append(names, name)
	}

	sort.Slice(names, func(i, j int) bool {
		a, b := series[names[i]].LastValue(), series[names[j]].LastValue()
		if a != b {
			return a > b
		}
		return names[i] < names[j]
	})

	if n > 0 && len(names) > n {
		names = names[:n]
	}
	return names
}
