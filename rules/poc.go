package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"go.uber.org/atomic"
)

type GroupOut struct {
	SeriesInPreviousEval []map[string]labels.Labels // One per Rule.
	StaleSeries          []labels.Labels
	EvaluationTime       time.Duration
	LastEvaluation       time.Time
	ShouldRestore        bool
	MarkStale            bool
	Rules                []Rule
}

type GroupIn struct {
	SeriesInPreviousEval []map[string]labels.Labels // One per Rule.
	StaleSeries          []labels.Labels
	EvaluationTime       time.Duration
	LastEvaluation       time.Time
	ShouldRestore        bool
	MarkStale            bool
	Rules                []RuleIn
}

type RuleOut struct {
	// The name of the alert.
	Name                string
	Restored            bool
	EvaluationDuration  time.Duration
	EvaluationTimestamp time.Time
	Health              RuleHealth
	LastError           string
	Active              map[uint64]*Alert
}

type RuleIn struct {
	// The name of the alert.
	Name                string
	Restored            bool
	EvaluationDuration  time.Duration
	EvaluationTimestamp time.Time
	Health              RuleHealth
	LastError           string
	Active              map[uint64]*Alert
}

func (g *Group) SetContext(ctx context.Context) {
	g.opts.Context = ctx
}

/*
func (g *Group) MarshalJSON() ([]byte, error) {
	return json.Marshal(&GroupOut{
		EvaluationTime:       g.evaluationTime,
		LastEvaluation:       g.lastEvaluation,
		Rules:                g.rules,
		SeriesInPreviousEval: g.seriesInPreviousEval,
		StaleSeries:          g.staleSeries,
		ShouldRestore:        g.shouldRestore,
		MarkStale:            g.markStale,
	})
}

func (r *AlertingRule) MarshalJSON() ([]byte, error) {
	errstr := ""
	if r.lastError != nil {
		errstr = r.lastError.Error()
	}
	return json.Marshal(&RuleOut{
		Name:                r.name,
		Restored:            r.restored,
		EvaluationDuration:  r.evaluationDuration,
		EvaluationTimestamp: r.evaluationTimestamp,
		Health:              r.health,
		LastError:           errstr,
		Active:              r.active,
	})
}

func (r *RecordingRule) MarshalJSON() ([]byte, error) {
	errstr := ""
	if r.lastError != nil {
		errstr = r.lastError.Error()
	}

	return json.Marshal(&RuleOut{
		Name:                r.name,
		EvaluationDuration:  r.evaluationDuration,
		EvaluationTimestamp: r.evaluationTimestamp,
		Health:              r.health,
		LastError:           errstr,
	})
}*/

func (g *Group) SerializeGroupPb() ([]byte, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()

	staleSeries := labelsArrayToPb(g.staleSeries)

	seriesInPreviousEval := make([]*prompb.LabelsPerSeries, len(g.seriesInPreviousEval))
	for idx, lbls := range g.seriesInPreviousEval {
		pb := labelsArrayToPb(MapValues(lbls))
		seriesInPreviousEval[idx] = &prompb.LabelsPerSeries{LabelsArray: pb}
	}

	rules := make([]*prompb.Rule, 0, len(g.Rules()))
	for _, rule := range g.Rules() {

		errstr := ""
		if rule.LastError() != nil {
			errstr = rule.LastError().Error()
		}

		pbRule := &prompb.Rule{
			Name:                rule.Name(),
			EvaluationDuration:  int64(rule.GetEvaluationDuration()),
			EvaluationTimestamp: rule.GetEvaluationTimestamp().UnixMilli(),
			Health:              prompb.RuleHealth(prompb.RuleHealth_value[string(rule.Health())]),
			LastError:           errstr,
		}
		if ar, ok := rule.(*AlertingRule); ok {
			pbRule.Restored = ar.restored.Load()
			pbRule.Active = make([]*prompb.Alert, len(ar.active))
			idx := 0
			for id, alert := range ar.active {
				a := &prompb.Alert{
					Id:          id,
					State:       prompb.AlertState(alert.State),
					Labels:      labelsToPb(alert.Labels),
					Annotations: labelsToPb(alert.Annotations),
					Value:       alert.Value,
					ActiveAt:    alert.ActiveAt.UnixMilli(),
					FiredAt:     alert.FiredAt.UnixMilli(),
					ResolvedAt:  alert.ResolvedAt.UnixMilli(),
					LastSentAt:  alert.LastSentAt.UnixMilli(),
					ValidUntil:  alert.ValidUntil.UnixMilli(),
				}
				pbRule.Active[idx] = a
				idx++
			}
		}
		rules = append(rules, pbRule)
	}

	pbGroup := &prompb.Group{
		EvaluationTime:       int64(g.evaluationTime),
		MarkStale:            g.markStale,
		ShouldRestore:        g.shouldRestore,
		LastEvaluation:       g.lastEvaluation.UnixMilli(),
		StaleSeries:          staleSeries,
		SeriesInPreviousEval: seriesInPreviousEval,
		Rules:                rules,
	}

	bytes, err := proto.Marshal(pbGroup)
	if err != nil {
		return nil, err
	}
	return snappy.Encode(nil, bytes), nil
}

func labelsArrayToPb(labelsArray []labels.Labels) []*prompb.Labels {
	staleSeries := make([]*prompb.Labels, len(labelsArray))
	for idx, lbls := range labelsArray {
		pb := labelsToPb(lbls)
		staleSeries[idx] = pb
	}
	return staleSeries
}

func labelsToPb(lbls labels.Labels) *prompb.Labels {
	pb := make([]prompb.Label, len(lbls))
	for idx, v := range lbls {
		pb[idx] = prompb.Label{Name: v.Name, Value: v.Value}
	}
	return &prompb.Labels{Labels: pb}
}

func pbLabelsToLabels(pb *prompb.Labels) labels.Labels {
	lbls := make([]labels.Label, len(pb.Labels))
	for idx, v := range pb.Labels {
		lbls[idx] = labels.Label{Name: v.Name, Value: v.Value}
	}
	return labels.Labels(lbls)
}

func pbLabelsArrayToLabelsArray(pb []*prompb.Labels) []labels.Labels {
	staleSeries := make([]labels.Labels, len(pb))
	for idx, lbls := range pb {
		staleSeries[idx] = pbLabelsToLabels(lbls)
	}
	return staleSeries
}

func (g *Group) SerializeGroup() ([]byte, error) {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	return json.MarshalIndent(g, "", "\t")
}

func MapValues(m map[string]labels.Labels) []labels.Labels {
	l := 0
	if m != nil {
		l = len(m)
	}
	r := make([]labels.Labels, 0, l)
	if m != nil {
		for _, v := range m {
			r = append(r, v)
		}
	}
	return r
}

func (g *Group) DeserializeGroupPb(bytes []byte) error {
	loc, _ := time.LoadLocation("UTC")
	bytes, err := snappy.Decode(nil, bytes)
	if err != nil {
		return err
	}
	pbg := &prompb.Group{}
	err = proto.Unmarshal(bytes, pbg)
	if err != nil {
		return err
	}
	g.evaluationTime = time.Duration(pbg.EvaluationTime)
	g.lastEvaluation = time.UnixMilli(pbg.LastEvaluation).In(loc)
	g.staleSeries = pbLabelsArrayToLabelsArray(pbg.StaleSeries)

	for idx, r := range g.rules {
		if idx >= len(pbg.Rules) {
			break
		}
		pbr := pbg.Rules[idx]

		if pbg.SeriesInPreviousEval[idx] != nil {
			lblsArr := pbLabelsArrayToLabelsArray(pbg.SeriesInPreviousEval[idx].LabelsArray)
			seriesReturned := make(map[string]labels.Labels, len(lblsArr))
			for _, lbls := range lblsArr {
				buf := [1024]byte{}
				seriesReturned[string(lbls.Bytes(buf[:]))] = lbls
			}
			g.seriesInPreviousEval[idx] = seriesReturned
		}

		if r.Name() == pbr.GetName() {
			r.SetEvaluationDuration(time.Duration(pbr.EvaluationDuration))
			r.SetEvaluationTimestamp(time.UnixMilli(pbr.EvaluationTimestamp).In(loc))
			r.SetHealth(RuleHealth(pbr.GetHealth().String()))
			if v := pbr.GetLastError(); v != "" {
				r.SetLastError(fmt.Errorf(v))
			}
			if ar, ok := r.(*AlertingRule); ok {
				ar.restored = atomic.NewBool(pbr.GetRestored())
				ar.active = make(map[uint64]*Alert, len(pbr.Active))
				for _, pba := range pbr.Active {
					a := &Alert{
						State:       AlertState(pba.GetState()),
						Labels:      pbLabelsToLabels(pba.GetLabels()),
						Annotations: pbLabelsToLabels(pba.GetAnnotations()),
						Value:       pba.Value,
						ActiveAt:    time.UnixMilli(pba.GetActiveAt()).In(loc),
						FiredAt:     time.UnixMilli(pba.GetFiredAt()).In(loc),
						ResolvedAt:  time.UnixMilli(pba.GetResolvedAt()).In(loc),
						LastSentAt:  time.UnixMilli(pba.GetLastSentAt()).In(loc),
						ValidUntil:  time.UnixMilli(pba.GetValidUntil()).In(loc),
					}
					ar.active[pba.Id] = a
				}
			}
		}
	}

	return nil
}

func (g *Group) DeserializeGroup(bytes []byte) error {

	ge := &GroupIn{}
	err := json.Unmarshal(bytes, &ge)
	if err != nil {
		return err
	}
	g.evaluationTime = ge.EvaluationTime
	g.lastEvaluation = ge.LastEvaluation
	g.staleSeries = ge.StaleSeries

	for idx, r := range g.rules {
		if idx >= len(ge.Rules) {
			break
		}
		er := ge.Rules[idx]
		g.seriesInPreviousEval[idx] = ge.SeriesInPreviousEval[idx]
		if r.Name() == er.Name {
			if ar, ok := r.(*AlertingRule); ok {
				ar.active = er.Active
				ar.evaluationDuration = atomic.NewDuration(er.EvaluationDuration)
				ar.evaluationTimestamp = atomic.NewTime(er.EvaluationTimestamp)
				ar.health = atomic.NewString(string(er.Health))
				ar.restored = atomic.NewBool(er.Restored)
				if er.LastError != "" {
					ar.lastError = atomic.NewError(fmt.Errorf(er.LastError)) // TODO?
				}
			} else if ar, ok := r.(*RecordingRule); ok {
				ar.evaluationDuration = atomic.NewDuration(er.EvaluationDuration)
				ar.evaluationTimestamp = atomic.NewTime(er.EvaluationTimestamp)
				ar.health = atomic.NewString(string(er.Health))
				if er.LastError != "" {
					ar.lastError = atomic.NewError(fmt.Errorf(er.LastError))
				}
			}
		}
	}

	return nil
}
