package mapreduce

func ReregisterWorker(mr *Master, workerAddress string) {
	mr.registerChannel <- workerAddress
}

func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)     // number of map tasks
		numOtherPhase = mr.nReduce // number of reducers
	case reducePhase:
		ntasks = mr.nReduce           // number of reduce tasks
		numOtherPhase = len(mr.files) // number of map tasks
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, numOtherPhase)

	completedTasks := make(chan int, ntasks)

	for i := 0; i < ntasks; i++ {
		go func(taskNum int) {
			taskCompleted := false
			for !taskCompleted {
				workerAddress := <-mr.registerChannel

				args := RunTaskArgs{
					JobName:       mr.jobName,
					File:          mr.files[i],
					Phase:         phase,
					TaskNumber:    taskNum,
					NumOtherPhase: numOtherPhase,
				}

				success := call(workerAddress, "Worker.RunTask", &args, nil)

				if success {
					go ReregisterWorker(mr, workerAddress)
					taskCompleted = true
					completedTasks <- taskNum
				}
			}
		}(i)
	}
	for i := 0; i < ntasks; i++ {
		<-completedTasks
	}

	debug("Schedule: %v phase finished\n", phase)
}
