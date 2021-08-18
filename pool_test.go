package pool_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/Capucinoxx/go-pool"
)

func createNjobs(count int, fn interface{}) []pool.Job {
	resp := make([]pool.Job, count)
	for i := 0; i < count; i++ {
		resp[i] = *pool.CreateJob([]byte(fmt.Sprintf("%d", i)), fn, i)
	}
	return resp
}

func TestPool(t *testing.T) {
	cases := map[string]struct {
		ctxFn        func() (context.Context, context.CancelFunc)
		fn           interface{}
		withDeadline bool
		hasData      bool
	}{
		"basic": {
			ctxFn: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.TODO())
			},
			fn:      func(ctx context.Context, i int) (int, error) { return i * 100, nil },
			hasData: true,
		},
		"with timeout": {
			ctxFn: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.TODO(), time.Nanosecond*10)
			},
			fn:           func(ctx context.Context, i int) (int, error) { return i * 100, nil },
			withDeadline: true,
		},
	}

	for name, tt := range cases {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := tt.ctxFn()
			defer cancel()

			p := pool.New(10)
			go p.Add(createNjobs(100, tt.fn)...)
			go p.Run(ctx)

			for {
				select {
				case r, ok := <-p.Results():
					if !ok {
						continue
					}

					values := r.(map[string]interface{})

					if err, ok := values["error"]; ok && err != nil {
						if !tt.withDeadline && err != context.DeadlineExceeded {
							t.Errorf("woupsi")
						}
					}

					if tt.hasData {
						data := values["r"].([]interface{})[0].(int)

						if fmt.Sprintf("%d", data/100) != string(values["task"].([]byte)) {
							t.Errorf("want: %s, have %v\n", values["task"], values["r"].([]interface{})[0].(int)/100)
						}
					}

				case <-p.Done:
					return
				}

			}

		})
	}
}
