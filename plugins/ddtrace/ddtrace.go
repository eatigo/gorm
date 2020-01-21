// code taken and modified from https://github.com/DataDog/dd-trace-go/blob/v1.20.1/contrib/jinzhu/gorm/gorm.go
// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2016-2020 Datadog, Inc.


package ddtrace

import (
	"context"
	"errors"
	"github.com/eatigo/gorm"
	"github.com/eatigo/gorm/plugins"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
	"math"
	"time"
)

var _ gorm.PluginInterface = &DDtrace{}

type DDtrace struct {
	Options []Option
}

const (
	gormContextKey       = "dd-trace-go:context"
	gormSpanStartTimeKey = "dd-trace-go:span"
)

func New(opts ...Option) (*DDtrace, error) {
	if len(opts)<0{
		return nil, errors.New("options must not be empty")
	}

	return &DDtrace{
		Options: opts,
	}, nil

}

// Apply apply reconnect to GORM DB instance
func (ddtrace *DDtrace) Apply(db *gorm.DB) {

	afterFunc := func(operationName string,analyticsRate float64, serviceName string) func(*gorm.Scope) {
		return func(scope *gorm.Scope) {
			after(scope, operationName,analyticsRate,serviceName)
		}
	}
	cfg := new(config)
	defaults(cfg)
	for _, fn := range ddtrace.Options {
		fn(cfg)
	}

	cb := db.Callback()
	cb.Create().Before("gorm:before_create").Register("dd-trace-go:before_create", before)
	cb.Create().After("gorm:after_create").Register("dd-trace-go:after_create",
		afterFunc(plugins.CreateCallback,cfg.analyticsRate,cfg.serviceName))
	cb.Update().Before("gorm:before_update").Register("dd-trace-go:before_update", before)
	cb.Update().After("gorm:after_update").Register("dd-trace-go:after_update", afterFunc(plugins.UpdateCallback,cfg.analyticsRate,cfg.serviceName))
	cb.Delete().Before("gorm:before_delete").Register("dd-trace-go:before_delete", before)
	cb.Delete().After("gorm:after_delete").Register("dd-trace-go:after_delete", afterFunc(plugins.DeleteCallback,cfg.analyticsRate,cfg.serviceName))
	cb.Query().Before("gorm:query").Register("dd-trace-go:before_query", before)
	cb.Query().After("gorm:after_query").Register("dd-trace-go:after_query", afterFunc(plugins.QueryCallback,cfg.analyticsRate,cfg.serviceName))
	cb.RowQuery().Before("gorm:row_query").Register("dd-trace-go:before_row_query", before)
	cb.RowQuery().After("gorm:row_query").Register("dd-trace-go:after_row_query", afterFunc(plugins.RowQueryCallback,cfg.analyticsRate,cfg.serviceName))

}

// WithContext attaches the specified context to the given db. The context will
// be used as a basis for creating new spans. An example use case is providing
// a context which contains a span to be used as a parent.
func (ddtrace *DDtrace) WithContext(ctx context.Context, db *gorm.DB) error {
	if db == nil {
		return errors.New("gorm is nil")
	}
	if ctx ==nil{
		ctx =context.Background()
	}
	db.Set(gormContextKey, ctx)

	return nil
}

func before(scope *gorm.Scope) {
	scope.Set(gormSpanStartTimeKey, time.Now())
}

func after(scope *gorm.Scope, operationName string, analyticsRate float64, serviceName string) {
	v, ok := scope.Get(gormContextKey)
	if !ok {
		return
	}
	ctx := v.(context.Context)


	v, ok = scope.Get(gormSpanStartTimeKey)
	if !ok {
		return
	}
	t, ok := v.(time.Time)

	opts := []ddtrace.StartSpanOption{
		tracer.StartTime(t),
		tracer.ServiceName(serviceName),
		tracer.SpanType(ext.SpanTypeSQL),
		tracer.ResourceName(scope.SQL),
	}
	if !math.IsNaN(analyticsRate) {
		opts = append(opts, tracer.Tag(ext.EventSampleRate, analyticsRate))
	}

	span, _ := tracer.StartSpanFromContext(ctx, operationName, opts...)
	span.Finish(tracer.WithError(scope.DB().Error))
}
