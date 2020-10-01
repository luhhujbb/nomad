package api

import (
	"context"
	"encoding/json"

	"github.com/hashicorp/nomad/api"
)

type Events struct {
	Index  uint64
	Events []Event
}

type Topic string

type Event struct {
	Topic      Topic
	Type       string
	Key        string
	FilterKeys []string
	Index      uint64
	Payload    interface{}
}

func (e *Events) IsHeartBeat() bool {
	return e.Index == 0 && len(e.Events) == 0
}

type JobEvent struct {
	Job api.Job
}

type EvalEvent struct {
	Eval api.Evaluation
}

type AllocEvent struct {
	Alloc api.Allocation
}

type DeploymentEvent struct {
	Deployment api.Deployment
}

type NodeEvent struct {
	Node api.Node
}

type EventStream struct {
	client *Client
}

func (c *Client) EventStream() *EventStream {
	return &EventStream{client: c}
}

func (e *EventStream) Stream(ctx context.Context, topics map[Topic][]string, index uint64, q *QueryOptions) (<-chan *Events, <-chan error) {

	errCh := make(chan error, 1)

	// reqPath := fmt.Sprintf("/v1/event/stream")
	r, err := e.client.newRequest("GET", "/v1/event/stream")
	if err != nil {
		errCh <- err
		return nil, errCh
	}
	// TODO(drew) need to do special param encoding here
	r.setQueryOptions(q)
	_, resp, err := requireOK(e.client.doRequest(r))

	if err != nil {
		errCh <- err
		return nil, errCh
	}

	eventsCh := make(chan *Events, 10)
	go func() {
		defer resp.Body.Close()

		dec := json.NewDecoder(resp.Body)

		for {
			select {
			case <-ctx.Done():
				close(eventsCh)
				return
			default:
			}

			// Decode next newline delimited json of events
			var events Events
			if err := dec.Decode(&events); err != nil {
				close(eventsCh)
				errCh <- err
				return
			}
			if events.IsHeartBeat() {
				continue
			}

			eventsCh <- &events

		}
	}()

	return eventsCh, errCh
}
