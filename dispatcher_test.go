package dispatcher

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"runtime"
	"testing"
)

func TestGoroutineLeaks(T *testing.T) {
	for i := 0; i < 100; i++ {
		localDispatcher := NewService().LaunchAsyncDispatcher()
		localDispatcher.Close()
	}

	runtime.GC()
	assert.Equal(T, 2, runtime.NumGoroutine())
}

func TestDispatchingTasks(T *testing.T) {
	localDispatcher := NewService().LaunchAsyncDispatcher()

	// Dispatch jobs
	sum := 0
	localDispatcher.AddTasksToDispatcher(100)
	for i := 1; i <= 100; i++ {
		localDispatcher.SendToDispatchSyncQueue(func() {
			defer localDispatcher.Done()
			sum++
		})
	}
	localDispatcher.WaitForTasksCompletion()
	localDispatcher.Close()

	assert.Equal(T, sum, 100)
}

func TestDispatcherError(T *testing.T) {
	localDispatcher := NewService().LaunchAsyncDispatcher()

	err := errors.New("error")

	localDispatcher.CatchErrorsInDispatcher(err)
	localDispatcher.Close()

	assert.Equal(T, "error", localDispatcher.GetDispatcherError().Error())
}

func TestDispatcherWithLimitedGoRoutines(T *testing.T) {
	localDispatcher := NewService().LaunchAsyncDispatcher()

	// Dispatch jobs
	sum := 0
	localDispatcher.AddTasksToDispatcher(100)
	localDispatcher.SetMaxAsyncTasks(5)
	for i := 1; i <= 100; i++ {
		localDispatcher.AcquireAsyncTask(1)
		localDispatcher.SendToDispatchSyncQueue(func() {
			defer localDispatcher.Done()
			defer localDispatcher.ReleaseAsyncTask(1)
			sum++
		})
	}
	localDispatcher.WaitForTasksCompletion()
	localDispatcher.Close()

	assert.Equal(T, sum, 100)
}

func TestDispatcherWithCodeGuard(T *testing.T) {
	localDispatcher := NewService().LaunchAsyncDispatcher()

	// Dispatch jobs
	sum := 0
	localDispatcher.AddTasksToDispatcher(100)
	for i := 1; i <= 100; i++ {
		localDispatcher.SendToDispatchSyncQueue(func() {
			defer localDispatcher.Done()
			localDispatcher.Guard()
			sum++
			localDispatcher.UnGuard()
		})
	}
	localDispatcher.WaitForTasksCompletion()
	localDispatcher.Close()

	assert.Equal(T, sum, 100)
}