package main

import "sync"

type Task struct {
	MainTask func() // main taks to execute
	Callback func() // optional callback function
	IsBlocking bool // flag to determine if a task is blocking or not
}

type CommandEventLoop struct {
	mainTask chan Task // channel to hold commands to be processed
	commandQueue chan Task // channel to hold calback tasks
	stop chan bool // channel to inidicate the event loop to stop	
}

func Add(eventLoop *CommandEventLoop, task *Task) {
	// push task to command channel
	eventLoop.mainTask <- *task
}

func AddToTaskQueue(eventLoop *CommandEventLoop, task *Task) {
	// push task to command queue
	eventLoop.commandQueue <- *task
}

func StopEventLoop(eventLoop *CommandEventLoop) {
	eventLoop.stop <- true
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
				case task := <-eventLoop.mainTask:
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
				case task := <- eventLoop.commandQueue:
				// ececute callback task
				task.MainTask()
				case stop := <- eventLoop.stop:
				if stop {
					return
				}
			}
		}
	}()
	return &wg
}