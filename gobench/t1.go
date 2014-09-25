package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rescrv/HyperDex/bindings/go/client"
)

var (
	msgs = flag.Int("msgs", 1, "number of messages to enq/deq")
	thrd = flag.Int("threads", 1, "number of threads to use")
	enq  = flag.Bool("enq", false, "enqueue?")
	deq  = flag.Bool("deq", false, "dequeue?")
)

func xlate(count *uint64) string {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], atomic.AddUint64(count, 1))
	return string(b[:])
}

var (
	searchTracker, delTracker *stats
	retries                   uint64
)

type stats struct {
	name            string
	size            int
	min, max, total time.Duration
	sync.Mutex
}

func newStats(name string) *stats {
	return &stats{name: name}
}

func (s *stats) add(latency time.Duration) {
	s.Lock()
	defer s.Unlock()

	if latency < s.min || s.min == 0 {
		s.min = latency
	}
	if latency > s.max {
		s.max = latency
	}
	s.total += latency
	s.size++
}

// for now, print to the screen. I know, I know...
func (s *stats) report() {
	s.Lock()
	fmt.Println("total time in", s.name+":", s.total)
	if s.size > 0 {
		fmt.Println("avg", s.name, "time:", time.Duration(int64(s.total)/int64(s.size)))
	}
	fmt.Println("min", s.name, "time:", s.min)
	fmt.Println("max", s.name, "time:", s.max)
	s.Unlock()
}

func Enqueue(count *uint64, db *client.Client) error {
	id := xlate(count)

	st := db.Put("messages", id, client.Attributes{"body": "goo"})

	if st.Status != client.SUCCESS {
		return errors.New("error inserting messages" + st.Error())
	}

	st = db.Put("handles", xlate(count), client.Attributes{"messageId": id})

	if st.Status != client.SUCCESS {
		return errors.New("error inserting handles" + st.Error())
	}

	return nil
}

var (
	ref    int32
	dqChan chan string // keys, for now
)

// spin this up in ensure or something per queue, spin down when queue goes empty
func getSome(db *client.Client) {
	go func() {
		chanMax := 512
		dqChan = make(chan string, chanMax) // arbitrary size, for now

		// This routine will constantly keep our buffer full up to cap or refs
		for { // for now, sit forever... eventually should spin down
			fmt.Println(ref)
			var lastKey string
			for atomic.LoadInt32(&ref) > 0 {

				max := uint32(math.Max(float64(atomic.LoadInt32(&ref)), float64(chanMax)))
				attrC, _ := db.SortedSearch("handles",
					[]client.Predicate{{"id", lastKey, client.GREATER_THAN}}, "id", max, "min")
				var n int
				now := time.Now()

				for attr := range attrC {
					key := attr["id"].(string)
					select {
					case dqChan <- key:
						lastKey = key
					default:
					}
					n++
				}
				// TODO better way to say no messages than trying a search for each?
				fmt.Println(time.Since(now))
				if n == 0 {
					dqChan <- ""
				}
			}
		}
	}()
}

func Dequeue(db *client.Client) error {
	var err error

	atomic.AddInt32(&ref, 1)
	defer atomic.AddInt32(&ref, -1)

	for ; ; atomic.AddUint64(&retries, 1) {
		then := time.Now()
		key := <-dqChan
		now := time.Since(then)

		if key == "" {
			fmt.Println("nokey", now)
			break
		}

		id := binary.BigEndian.Uint64([]byte(key))
		fmt.Println("getkey", now, id)

		then = time.Now()
		st := db.Del("handles", key)
		fmt.Println("deltime", time.Since(then))
		//delTracker.add(time.Since(now))

		if st.Status == client.NOTFOUND {
			continue // try to get another, we got beat
			atomic.AddUint64(&retries, 1)
		} else if st.Status != client.SUCCESS {
			return errors.New("failed to delete handle" + st.Error())
			// TODO we should try to get the messages for the handles so
			// far or attempt to put them back on the queue.
		}

		break
	}

	// for now, get one at a time, could get each concurrently

	//_, st := db.Get("messages", messageId)

	//if st.Status != client.SUCCESS {
	//err = errors.New("dequeue error getting message" + st.Error())
	//}

	return err
}

func main() {
	flag.Parse()

	// gotta parse flags to get msgs
	searchTracker = newStats("search")
	delTracker = newStats("del")

	c, err, errChan := client.NewClient("127.0.0.1", 1982)
	var count uint64 = 0
	start := time.Now()

	go func() {
		for err := range errChan {
			if err.Error() != "" {
				fmt.Println(err)
			}
		}
	}()

	pipes := *thrd
	var wait sync.WaitGroup

	if *enq {

		mChan := make(chan struct{}, *msgs)
		go func() {
			for i := 0; i < *msgs; i++ {
				mChan <- struct{}{}
			}
			close(mChan)
		}()

		wait.Add(pipes)
		for i := 0; i < pipes; i++ {
			go func() {
				defer wait.Done()
				for _ = range mChan {
					err = Enqueue(&count, c)
					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
				}
			}()
		}
	}

	if *deq {

		getSome(c)

		nmsg := *msgs
		dChan := make(chan struct{}, nmsg)
		go func() {
			for i := 0; i < nmsg; i++ {
				dChan <- struct{}{}
			}
			close(dChan)
		}()

		wait.Add(pipes)
		for i := 0; i < pipes; i++ {
			go func() {
				defer wait.Done()
				for _ = range dChan {
					err = Dequeue(c)

					if err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
				}
			}()
		}
	}

	wait.Wait()

	fmt.Println("total time for all", time.Since(start))
	fmt.Println("total retries", retries)
	//searchTracker.report()
	//delTracker.report()
}
