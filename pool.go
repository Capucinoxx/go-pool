package pool

import (
	"context"
	"fmt"
	"reflect"
	"sync"
)

//
type Job struct {
	task   []byte
	fn     interface{}
	args   interface{}
	result interface{}
}

//
func CreateJob(task []byte, fn, args interface{}) *Job {
	return &Job{task: task, fn: fn, args: args}
}

//
func (j *Job) exec(ctx context.Context) *Job {
	fn := reflect.ValueOf(j.fn)
	in := reflect.ValueOf(j.args)

	typs := make([]reflect.Type, 0)
	if in.Kind() == reflect.Slice {
		for i := 0; i < in.Len(); i++ {
			typs = append(typs, in.Index(i).Type())
		}
	} else {
		typs = []reflect.Type{in.Type()}
	}

	if err := validateFunc(fn, typs); err != nil {
		return nil
	}

	j.result = formatValues(fn.Call([]reflect.Value{reflect.ValueOf(ctx), in}))
	return j
}

func validateFunc(fn reflect.Value, in []reflect.Type) error {
	if fn.Type().NumIn() != len(in)+1 {
		return nil
	}

	if fn.Type().NumOut() != 2 {
		return nil
	}

	if !fn.Type().In(0).Implements(reflect.TypeOf((*error)(nil)).Elem()) ||
		!fn.Type().In(1).Implements(reflect.TypeOf((*error)(nil)).Elem()) {
		return nil
	}

	return nil
}

func formatValues(data []reflect.Value) interface{} {
	resp := make([]interface{}, len(data))
	for i, v := range data {
		resp[i] = v.Interface()
	}
	return resp
}

type pool struct {
	count   int
	jobs    chan Job
	results chan interface{}
	Done    chan struct{}
}

//
func New(workerCount int) pool {
	return pool{
		count:   workerCount,
		jobs:    make(chan Job, workerCount),
		results: make(chan interface{}, workerCount),
		Done:    make(chan struct{}),
	}
}

//
func (p pool) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for w := 0; w < p.count; w++ {
		wg.Add(1)

		go worker(ctx, &wg, p.jobs, p.results)
	}

	wg.Wait()
	close(p.Done)
	close(p.results)
}

//
func (p pool) Results() <-chan interface{} { return p.results }

//
func (p pool) Add(jobs ...Job) {
	for i := range jobs {
		p.jobs <- jobs[i]
	}

	close(p.jobs)
}

func worker(ctx context.Context, wg *sync.WaitGroup, jobs <-chan Job, results chan<- interface{}) {
	defer wg.Done()

	for {
		select {
		case job, ok := <-jobs:
			if !ok {
				return
			}
			results <- map[string]interface{}{"task": job.task, "r": job.exec(ctx).result}
		case <-ctx.Done():
			fmt.Printf("cancelled worker. Error detail: %v\n", ctx.Err())
			results <- map[string]interface{}{"error": ctx.Err()}
			return
		}
	}
}
