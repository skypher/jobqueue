package jobqueue

import(
	"testing"

	"fmt"
	"time"
)

func TestOpenClose(t *testing.T) {
	jq, err := OpenJobQueue("/tmp/jobqueue")
	if err != nil { t.Error(err) }
	defer jq.Close()
}

func makeWorker(job Job) Worker {
	return func() {
		fmt.Println("worker started", job.Data)
		time.Sleep(1 * time.Second)
		fmt.Println("worker finished", job.Data)
	}
}

func TestStart(t *testing.T) {
	jq, err := OpenJobQueue("/tmp/jobqueue")
	if err != nil { t.Error(err) }
	jq.Start(makeWorker)
	for i:=1; i <= 10; i++ {
		jq.EnqueueJob(Job{Data:i}, 0)
	}
	time.Sleep(30 * time.Second)
	defer jq.Close()
}

/*
func TestSetMaxWorkers(t *testing.T) {
	jq, err := OpenJobQueue("/tmp/jobqueue")
	if err != nil { t.Error(err) }
	jq.Start(makeWorker)
//	jq.SetMaxWorkers(3)
	defer jq.Close()
}
*/
