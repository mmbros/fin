package taskengine

import "sort"

type mapTaskWorkers map[TaskID][]WorkerID

type sortTaskItem struct {
	tid     TaskID
	workers int
}

// byWorkers type is used to sort the taskID array
// by num of workers in ascending order
type byWorkers []sortTaskItem

func (a byWorkers) Len() int      { return len(a) }
func (a byWorkers) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// func (a byLen) Less(i, j int) bool { return a[i].workers < a[j].workers }
func (a byWorkers) Less(i, j int) bool {
	// NOTE: the == case is handled for test purpose only,
	//       in order to make the sorted indipendent by the initial order.
	//       It is (probably) not mandatory!
	if a[i].workers == a[j].workers {
		return a[i].tid < a[j].tid
	}
	return a[i].workers < a[j].workers
}

// getTaskWorkers returns the mapTaskWorkers object
// derived from the WorkerTasks object in input
func (wts WorkerTasks) taskWorkers() mapTaskWorkers {
	tws := mapTaskWorkers{}
	for wid, ts := range wts {
		for _, t := range ts {
			tid := t.TaskID()
			if ws, ok := tws[tid]; ok {
				tws[tid] = append(ws, wid)
			} else {
				tws[tid] = []WorkerID{wid}
			}
		}
	}
	return tws
}

// getSortedTasksByLessWorkers returns the array of TaskID
// ordered with the task with fewer workers first.
func (tws mapTaskWorkers) getSortedTasksByLessWorkers() []TaskID {

	// creates the byWorkers list
	var list = make(byWorkers, 0, len(tws))
	for t, ws := range tws {
		list = append(list, sortTaskItem{tid: t, workers: len(ws)})
	}
	// sort list by num of workers (in ascending order)
	sort.Sort(list)

	// build and return the array of TaskID
	a := make([]TaskID, 0, len(tws))
	for _, item := range list {
		a = append(a, item.tid)
	}
	return a
}

// SortTasksByLessWorkers reorder each worker tasks list, to handle globally each task as soon as possible.
// It gives priority to the task less handled by other workers.
func (wts WorkerTasks) SortTasksByLessWorkers() {
	// initialize the WorkerTaks result
	dst := WorkerTasks{}
	for w := range wts {
		dst[w] = Tasks{}
	}

	for {
		tws := wts.taskWorkers()

		tids := tws.getSortedTasksByLessWorkers()
		if len(tids) == 0 {
			break
		}

		for _, tid := range tids {

			// list of workers of the task
			wids := tws[tid]

			// Select the worker with the dst shorter list of tasks.
			// NOTE: In case of workers that have the same list of tasks length ('l == minlen'),
			//       picks the worker with lower workerId,
			//       in order to make the result deterministic.
			var minlen, minidx int
			for idx, wid := range wids {
				l := len(dst[wid])
				// NOTE: the 'l == minlen' case is handled for test purpose only,
				//       in order to make the sorted indipendent by the initial order.
				//       It is (probably) not mandatory!
				if idx == 0 || l < minlen || (l == minlen && wid < wids[minidx]) {
					minlen = l
					minidx = idx
				}
			}
			wid := wids[minidx]

			// remove the worker from the task's worker list
			wids[minidx] = wids[len(wids)-1]
			wids = wids[:len(wids)-1]
			tws[tid] = wids

			// remove the task from the src worker's tasks list
			// and insert into the dst worker's tasks list
			ts := wts[wid]
			for idx, t := range ts {
				if t.TaskID() == tid {
					// insert the task in dst
					dst[wid] = append(dst[wid], t)
					// remove the task from src
					ts[idx] = ts[len(ts)-1]
					ts = ts[:len(ts)-1]
					wts[wid] = ts

					break
				}
			}
		}
	}

	// update wts
	for wid := range wts {
		wts[wid] = dst[wid]
	}
}
