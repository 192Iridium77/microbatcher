package microbatcher

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type JobResult struct {
	Result interface{} // empty interface, exported for use outside of this module
	Err error // built in error interface
}

// a function that accepts jobs as a slice of empty interfaces and returns a slice of job results
type BatchProcessor func(jobs []interface{}) []JobResult

type MicroBatcher struct {
	batchProcessor BatchProcessor
	batchSize int
	batchFrequency time.Duration
	jobs chan interface{}
	wg sync.WaitGroup
	ctx context.Context
	cancel context.CancelFunc
}

/*
factory function that returns a pointer to the initialised microbatcher
*/
func CreateMicroBatcher(batchProcessor BatchProcessor, batchSize int, batchFrequency time.Duration) *MicroBatcher {
	ctx, cancel := context.WithCancel(context.Background());

	return &MicroBatcher{
		batchProcessor: batchProcessor,
		batchSize: batchSize,
		batchFrequency: batchFrequency,
		ctx: ctx,
		jobs: make(chan interface{}, batchSize),
		cancel: cancel,
	}
}

// METHODS

func (microbatcher *MicroBatcher) SubmitJob(job interface{}) *JobResult {
	select {
	case microbatcher.jobs <- job: // jobs channel can accept new jobs
		return nil
	default: // jobs channel is full, let's process
		results := microbatcher.processJobs()
		microbatcher.jobs <- job // now that it's processed, send a new job to the jobs channel

		return &JobResult{Err: results[0].Err}
	}
}

func (microbatcher *MicroBatcher) Start() {
	// add the start job to the wait group
	microbatcher.wg.Add(1)

	// manage the process on a go routine
	go func () {
		// defering Done lets the waitgroup know when this function returns or errors
		defer microbatcher.wg.Done()
		ticker := time.NewTicker(microbatcher.batchFrequency)
		defer ticker.Stop() // stop the ticker when this function is done

		// handle the ticker events
		for {
			select {
			case <- ticker.C: // ticker channel C recieves
				microbatcher.processJobs()
			case <- microbatcher.ctx.Done():
				return;
			}
		}
	}()
}

func run(microbatcher *MicroBatcher, jobs []interface{}) []JobResult {
	results := microbatcher.batchProcessor(jobs)

	for _, result := range results {
		fmt.Println(result.Err);
	}

	return results
}

func (microbatcher *MicroBatcher) processJobs() []JobResult {
	jobs := make([]interface{}, 0, microbatcher.batchSize);

	for {
		select {
		case job := <- microbatcher.jobs:
			jobs = append(jobs, job) // append jobs submitted to the channel until we reach the batchSize
			if len(jobs) == microbatcher.batchSize {
				run(microbatcher, jobs)
			}
		default: // channel can't accept jobs (must be full)
			run(microbatcher, jobs)
		}
	}
}

func (microbatcher *MicroBatcher) Shutdown() {
	microbatcher.cancel() // cancel the job using the context with cancel
	microbatcher.wg.Wait() // wait for the start function to finish
	close(microbatcher.jobs) // shuts down the channel after the last event is recieved
	microbatcher.processJobs() // process any remaining jobs
}