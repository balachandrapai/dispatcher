// Package dispatcher provides capabilities to launch an async dispatcher service
// using this service, one can execute tasks either serially by using the sync queue or concurrently.
package dispatcher

import (
	"context"
	"golang.org/x/sync/semaphore"
	"sync"
)

// Service is a wrapper around Dispatcher.
type Service struct {
}

func NewService() *Service {
	return &Service{}
}

// Dispatcher struct
type Dispatcher struct {
	queue     chan func()
	errors    chan error
	success   chan bool
	waitGroup sync.WaitGroup
	mutex     sync.Mutex
	sem       *semaphore.Weighted
	err 		error
}

// SetMaxAsyncTasks function is used to limit the number of go routines threads.
func (d *Dispatcher) SetMaxAsyncTasks(poolSize int64) {
	d.sem = semaphore.NewWeighted(poolSize)
}

// AcquireAsyncTask function is used to acquire the async tasks. It locks the go routines on the task.
// This function is used only when there is a maximum limit set on the number of go routines using SetMaxAsyncTasks.
// The function acquires an async task with a weight of i, blocking until resources are available.
func (d *Dispatcher) AcquireAsyncTask(i int) {
	_  = d.sem.Acquire(context.Background(), int64(i))
}

// ReleaseAsyncTask function is used to release the async task after execution.
// This function is used to release the lock executed by the AcquireAsyncTask.
// The function releases an async task with a weight of i.
func (d *Dispatcher) ReleaseAsyncTask(i int) {
	d.sem.Release(int64(i))
}

// SendToDispatchSyncQueue function is used to send the function inside the dispatcher.
// The function sends the function to the queue, which executes the function serially.
func (d *Dispatcher) SendToDispatchSyncQueue(function func()) {
	d.queue <- function
}

// CatchErrorsInDispatcher function is used to send the function inside the dispatcher.
func (d *Dispatcher) CatchErrorsInDispatcher(err error) {
	d.errors <- err
}

// newDispatcher is used to instantiate the Dispatcher
func newDispatcher(queue chan func(), errors chan error, success chan bool) *Dispatcher {
	return &Dispatcher{queue: queue, errors: errors, success: success}
}

// LaunchAsyncDispatcher starts a new dispatcher service.
// The dispatcher is used to execute all the Go routines. It handles the errors from the Go routines,
// notifies the main thread on completion of all the tasks dispatched.
//
// The Dispatcher contains three channels :
// A Queue for synchronous execution of tasks. A queue takes any function and executes it in a FIFO manner.
// An Errors channel for listening to any errors during the execution of the dispatcher.
// A Success channel to notify the successful completion of the dispatched tasks.
func (s *Service) LaunchAsyncDispatcher() *Dispatcher {
	// Create a queue channel which reads the function
	queue := make(chan func())
	// Create an error channel which reads the errors
	errs := make(chan error)
	// Dispatch queue success
	success := make(chan bool)

	// Instantiate a new dispatch queue
	dq := newDispatcher(queue, errs, success)

	// Spawn go routine to read and run functions in the queue channel
	go func() {
		for {
			nextFunction, open := <-dq.queue
			if !open {
				return
			}
			nextFunction()
		}
	}()

	// Spawn go routine to keep the errors channel unblocked
	go func() {
		for {
			var open bool
			dq.err, open = <-dq.errors
			if !open {
				return
			}
		}
	}()

	// Spawn go routine to keep the success channel unblocked
	go func() {
		for {
			_, open := <-dq.success
			if !open {
				return
			}
		}
	}()

	return dq
}

// AddTasksToDispatcher is used to add the number of tasks to be added to the dispatcher.
func (d *Dispatcher) AddTasksToDispatcher(i int) {
	d.waitGroup.Add(i)
}

// Done is used to notify the Dispatcher that a Go routine has finished execution
func (d *Dispatcher) Done() {
	d.waitGroup.Done()
}

// Guard is used to Guard the code
func (d *Dispatcher) Guard() {
	d.mutex.Lock()
}

// UnGuard is used to revoke the Guard
func (d *Dispatcher) UnGuard() {
	d.mutex.Unlock()
}

// WaitForTasksCompletion stops the thread execution until all the queue tasks are processed
func (d *Dispatcher) WaitForTasksCompletion() {
	d.waitGroup.Wait()
	d.success <- true
}

// GetDispatcherError gets the status of the instance of Dispatcher.
// The error in the dispatcher is caught and returned upon calling the callback function.
func (d *Dispatcher) GetDispatcherError() error {
	return d.err
}

// Close function closes all the channels launched for the particular Dispatcher instance.
// NOTE : If the Close() function is not called then the dispatcher will be
// garbage collected
func (d *Dispatcher) Close() {
	close(d.queue)
	close(d.errors)
	close(d.success)
}
