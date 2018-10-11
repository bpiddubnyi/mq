package mq

import (
	"bytes"
	"fmt"
	"strconv"
	"testing"
)

func TestMQ(t *testing.T) {
	var sub *Subscription
	var mq = New()

	t.Run("subscribe", func(t *testing.T) {
		sub = mq.Subscribe("abc")
		if sub == nil {
			t.Error("mq.Subscribe() returned nil")
		}
		if mq.Topics() != 1 || mq.Subscriptions() != 1 {
			t.Errorf("got %d topics and %d subscriptions instead of 1, 1", mq.Topics(), mq.Subscriptions())
		}
	})

	t.Run("publish/poll", func(t *testing.T) {
		for i := 0; i < 500; i++ {
			mq.Publish("abc", []byte(strconv.FormatInt(int64(i), 10)))
		}

		for i := 0; i < 500; i++ {
			msg, err := mq.Poll("abc", sub)
			if err != nil {
				t.Errorf("mq.Poll returned err = %s", err)
			}
			if !bytes.Equal(msg, []byte(strconv.FormatInt(int64(i), 10))) {
				t.Errorf("mq.Poll returned '%s' instead of '%s'", msg, strconv.FormatInt(int64(i), 10))
			}
		}
		mq.Unsubscribe("abc", sub)

		if mq.Topics() != 0 || mq.Subscriptions() != 0 {
			t.Errorf("got %d topics and %d subscriptions instead of 0, 0", mq.Topics(), mq.Subscriptions())
		}

		var subs = []*Subscription{}
		for i := 0; i < 100; i++ {
			s := mq.Subscribe("abc")
			if s == nil {
				t.Error("mq.Subscribe() returned nil")
			}
			subs = append(subs, s)
		}

		if mq.Topics() != 1 || mq.Subscriptions() != 100 {
			t.Errorf("got %d topics and %d subscriptions instead of 1, 100", mq.Topics(), mq.Subscriptions())
		}

		t.Run("parallel/publish", func(t *testing.T) {
			t.Parallel()
			for i := 0; i < 10000; i++ {
				mq.Publish("abc", []byte(strconv.FormatInt(int64(i), 10)))
			}
		})

		for sI, s := range subs {
			localSub := s
			t.Run(fmt.Sprintf("parallel/poll-%d", sI), func(t *testing.T) {
				t.Parallel()
				i := 0
				for i < 10000 {
					msg, err := mq.Poll("abc", localSub)
					if err != nil {
						t.Errorf("mq.Poll returned err = %s", err)
					}
					if msg == nil {
						continue
					}
					if !bytes.Equal(msg, []byte(strconv.FormatInt(int64(i), 10))) {
						t.Errorf("mq.Poll returned '%s' instead of '%s'", msg, strconv.FormatInt(int64(i), 10))
					}
					i++
				}

				msg, err := mq.Poll("abc", localSub)
				if msg != nil || err != nil {
					t.Errorf("mq.Poll() returned (%s, %s) instead of (nil, nil)", msg, err)
				}
				mq.Unsubscribe("abc", localSub)
			})
		}
	})

	t.Run("finish", func(t *testing.T) {
		if mq.Topics() != 0 || mq.Subscriptions() != 0 {
			t.Errorf("got %d topics and %d subscriptions instead of 0, 0", mq.Topics(), mq.Subscriptions())
		}
		if _, err := mq.Poll("abc", &Subscription{}); err != NoTopicError {
			t.Errorf("mq.Unsubscribe returned err = %s instead of %s", err, NoTopicError)
		}
	})
}
