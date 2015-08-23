package mapreduce

import (
	"fmt"
	"log"
)

// taskTracker tracks what tasks are waiting to be ran and tasks which are currently
// running
type taskTracker struct {
	queue     []string            // TODO: what are we queueing? Must be some task definition for map or reduce
	running   map[string]struct{} // TODO: make this a map taskID -> worker?
	completed map[string]struct{} // TODO: track some stats?
	failed    map[string]struct{}
}

type event struct {
	host  string
	state string // TODO: consts
	err   error  // if there was an error
}

func (e event) String() string {
	return fmt.Sprintf("[event host=%q state=%q err=%v]", e.host, e.state, e.err)
}

type masterTask struct {
	mapWorkers []*mapWorker

	mapEvents chan event
	quit      chan struct{}
}

func (m *masterTask) eventHandler() {
	defer log.Println("eventHandler stopped")

	for {
		select {
		case e := <-m.mapEvents:
			log.Printf("got map event: %s\n", e)
		case <-m.quit:
			log.Println("closing...")
			return
		}
	}
}
