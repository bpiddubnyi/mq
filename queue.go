package mq

import "container/list"

const (
	NoSubError       QueueError = "nosub"
	NoTopicError     QueueError = "notop"
	InvalidDataError QueueError = "invalid"
)

type Subscription struct {
	nextMsg *list.Element
}

type QueueError string

func (e QueueError) Error() string {
	return string(e)
}

type msg struct {
	data []byte
	subs uint64
}

type queue struct {
	subs      map[*Subscription]struct{}
	emptySubs map[*Subscription]struct{}
	queue     *list.List
}

func newQueue() queue {
	return queue{
		subs:      map[*Subscription]struct{}{},
		emptySubs: map[*Subscription]struct{}{},
		queue:     list.New(),
	}
}

func (q *queue) addSub(sub *Subscription) {
	q.subs[sub] = struct{}{}
	q.emptySubs[sub] = struct{}{}
}

func (q *queue) delSub(sub *Subscription) error {
	for {
		m, err := q.readMsg(sub)
		if err != nil {
			return err
		}

		if m == nil {
			break
		}
	}

	delete(q.subs, sub)
	delete(q.emptySubs, sub)
	return nil
}

func (q *queue) isEmpty() bool {
	return len(q.subs) == 0
}

func (q *queue) queueMsg(data []byte) {
	m := &msg{data: data, subs: uint64(len(q.subs))}
	e := q.queue.PushBack(m)

	for s := range q.emptySubs {
		s.nextMsg = e
		delete(q.emptySubs, s)
	}
}

func (q *queue) readMsg(sub *Subscription) ([]byte, error) {
	if sub.nextMsg == nil {
		return nil, nil
	}

	m, ok := sub.nextMsg.Value.(*msg)
	if !ok {
		// This should actually never happen, but error is better then panic anyway
		return nil, InvalidDataError
	}
	prevMsg := sub.nextMsg
	sub.nextMsg = sub.nextMsg.Next()

	if sub.nextMsg == nil {
		q.emptySubs[sub] = struct{}{}
	}

	m.subs--
	if m.subs == 0 {
		q.queue.Remove(prevMsg)
	}

	return m.data, nil
}
