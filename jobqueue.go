package jobqueue

import(
	"fmt"
    "path/filepath"

	"github.com/skypher/goque"
)

type Job struct {
	Data interface{} //must be serializable by gob; no channels and functions
}
type Worker func()
type WorkerFactory func(Job) Worker

type jobQueue struct {
	backlog *goque.PriorityQueue
	//transit *goque.Set //TODO unused as of now; need to implement disk-persistent Set in goque.

	makeWorker WorkerFactory

	setMaxWorkers chan int
	getMaxWorkers chan int
	getNumWorkers chan int
	jobsUpdated chan interface{}
	jobFinished chan interface{}
}

func (jq jobQueue) managePool(maxWorkers int) {
	fmt.Printf("managePool started, maxWorkers=%d\n", maxWorkers)
	numWorkers := 0
	for {
		select {
		case <-jq.getMaxWorkers:
			jq.getMaxWorkers <- maxWorkers
		case <-jq.getMaxWorkers:
			jq.getNumWorkers <- numWorkers
		case n := <-jq.setMaxWorkers:
			fmt.Printf("set maxWorkers=%d\n", maxWorkers)
			maxWorkers = n
		case <-jq.jobsUpdated:
			fmt.Println("jobsUpdated")
			if numWorkers < maxWorkers {
				item, err := jq.backlog.Dequeue()
				if err != nil && err != goque.ErrEmpty {
					panic(err)
				}
				if err == goque.ErrEmpty {
					fmt.Println("note: job queue empty")
				} else {
					var job Job
					err = item.ToObject(&job)
					if err != nil {
						panic(err)
					}
					worker := jq.makeWorker(job)
					go func() {
						worker()
						jq.jobFinished <- nil
					}()
					numWorkers++
					fmt.Printf("made and started worker, numWorkers=%d\n", numWorkers)
				}
			}
		case <-jq.jobFinished:
			numWorkers--
			fmt.Printf("job finished, numWorkers=%d\n", numWorkers)
			go func() { jq.jobsUpdated <- nil }()
		}
	}
}

func (jq jobQueue) EnqueueJob(job Job, prio uint8) {
	jq.backlog.EnqueueObject(prio, job)
	jq.jobsUpdated <- nil
}

func OpenJobQueue(datadir string) (*jobQueue, error) {
    backlog, err := goque.OpenPriorityQueue(filepath.Join(datadir, "backlog"), goque.ASC)
    if err != nil { return nil, err }

	//transit := nil // FIXME

	jq := jobQueue{
		backlog: backlog,
		setMaxWorkers: make(chan int),
		getMaxWorkers: make(chan int),
		getNumWorkers: make(chan int),
		jobsUpdated: make(chan interface{}),
		jobFinished: make(chan interface{}),
	}

    return &jq, nil
}

// TODO: pause/resume/stop queue processing
func (jq jobQueue) Start(makeWorker WorkerFactory) {
	jq.makeWorker = makeWorker
	go jq.managePool(5) // TODO default?
}

func (jq jobQueue) SetMaxWorkers(maxWorkers int) {
	jq.setMaxWorkers <- maxWorkers
}

func (jq jobQueue) Close() {
	// TODO what happens to the running workers?
	// graceful / forced shutdown types
	jq.backlog.Close()
	//jq.transit.Close()
}

