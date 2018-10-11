package mq

import (
	"sync"
)

type MQ struct {
	queues map[string]queue
	qLock  sync.Mutex
}

func New() *MQ {
	return &MQ{
		queues: map[string]queue{},
		qLock:  sync.Mutex{},
	}
}

func (mq *MQ) Subscribe(topic string) *Subscription {
	mq.qLock.Lock()
	defer mq.qLock.Unlock()

	q, ok := mq.queues[topic]
	if !ok {
		q = newQueue()
	}

	sub := &Subscription{}
	q.addSub(sub)
	mq.queues[topic] = q

	return sub
}

func (mq *MQ) Unsubscribe(topic string, sub *Subscription) {
	mq.qLock.Lock()
	defer mq.qLock.Unlock()

	q, ok := mq.queues[topic]
	if !ok {
		return
	}

	q.delSub(sub)
	if q.isEmpty() {
		delete(mq.queues, topic)
	}

}

func (mq *MQ) Publish(topic string, msg []byte) {
	mq.qLock.Lock()
	defer mq.qLock.Unlock()

	q, ok := mq.queues[topic]
	if !ok {
		return
	}

	q.queueMsg(msg)
}

func (mq *MQ) Poll(topic string, sub *Subscription) ([]byte, error) {
	mq.qLock.Lock()
	defer mq.qLock.Unlock()

	q, ok := mq.queues[topic]
	if !ok {
		return nil, NoTopicError
	}

	return q.readMsg(sub)
}

func (mq *MQ) Topics() int {
	return len(mq.queues)
}

func (mq *MQ) Subscriptions() int {
	n := 0
	for _, q := range mq.queues {
		n += len(q.subs)
	}
	return n
}
