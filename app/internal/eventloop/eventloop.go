package eventloop

import "sync"

type Task struct {
	MainTask   func() // main task to execute
	Callback   func() // optional callback function
	IsBlocking bool   // flag to determine if a task is blocking or not
}

type CommandEventLoop struct {
	MainTask     chan Task // channel to hold commands to be processed
	CommandQueue chan Task // channel to hold calback tasks
	Stop         chan bool // channel to indicate the event loop to stop
}

func Add(eventLoop *CommandEventLoop, task *Task) {
	// push task to command channel
	eventLoop.MainTask <- *task
}

func AddToTaskQueue(eventLoop *CommandEventLoop, task *Task) {
	// push task to command queue
	eventLoop.CommandQueue <- *task
}

func StopEventLoop(eventLoop *CommandEventLoop) {
	eventLoop.Stop <- true
}

func InitEventLoop(eventLoop *CommandEventLoop, workerPoolSize int) *sync.WaitGroup {
	wg := sync.WaitGroup{}

	// add event loop goroutine to the wait group
	wg.Add(1)
	workerPool := make(chan struct{}, workerPoolSize)

	// start the event loop
	go func() {
		defer wg.Done() // wait until event loop is finished

		for {
			select {
			case task := <-eventLoop.MainTask:
				if task.IsBlocking {
					// append blocking tasks to worker pool
					workerPool <- struct{}{} // acquire a worker

					// execute blocking task in separate go routine
					go func() {
						defer func() {
							<-workerPool // release the worker back to the pool
						}()
						task.MainTask()
						if task.Callback != nil {
							// If callback exists, run it after main task is completed
							AddToTaskQueue(eventLoop, &Task{
								MainTask: task.Callback,
							})
						}
					}()
				} else {
					// handle non blocking tasks
					task.MainTask()
				}
			case task := <-eventLoop.CommandQueue:
				// execute callback task
				task.MainTask()
			case stop := <-eventLoop.Stop:
				if stop {
					return
				}
			}
		}
	}()
	return &wg
}
