package taskengine

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestTaskWorkers(t *testing.T) {

	lessFunc := func(a, b WorkerID) bool { return a < b }

	type testCase struct {
		input WorkerTasks
		want  mapTaskWorkers
	}

	testCases := map[string]testCase{
		"test case 1": {
			input: WorkerTasks{
				"w1": {&testCaseTask{"t3", 30, true}, &testCaseTask{"t2", 20, true}, &testCaseTask{"t1", 10, true}},
				"w2": {&testCaseTask{"t3", 20, true}, &testCaseTask{"t2", 10, true}},
				"w3": {&testCaseTask{"t3", 10, true}},
			},
			want: mapTaskWorkers{
				"t1": {"w1"},
				"t2": {"w1", "w2"},
				"t3": {"w1", "w2", "w3"},
			},
		},
		"test case 2": {
			input: WorkerTasks{
				"w1": {&testCaseTask{"tt", 10, true}},
				"w2": {&testCaseTask{"tt", 10, true}},
				"w3": {&testCaseTask{"tt", 10, true}},
				"w4": {&testCaseTask{"tt", 10, true}},
				"w5": {&testCaseTask{"tt", 10, true}},
			},
			want: mapTaskWorkers{
				"tt": {"w5", "w2", "w3", "w4", "w1"},
			},
		}}

	copts := cmp.Options{
		// cmpopts.SortMaps(lessFunc),
		cmpopts.SortSlices(lessFunc),
	}
	for title, tc := range testCases {
		got := tc.input.taskWorkers()
		if diff := cmp.Diff(tc.want, got, copts); diff != "" {
			t.Errorf("%s: mismatch (-want +got):\n%v", title, diff)
		}
	}
}

func TestGetSortedTasksByLessWorkers(t *testing.T) {
	type testCase struct {
		input mapTaskWorkers
		want  []TaskID
	}

	testCases := map[string]testCase{
		"tasks with different number of workers": {
			input: mapTaskWorkers{
				"t1": {"w1"},
				"t2": {"w1", "w2"},
				"t3": {"w1", "w2", "w3"},
			},
			want: []TaskID{"t1", "t2", "t3"},
		},
		"all tasks with the same number of workers": {
			input: mapTaskWorkers{
				"t1": {"w1", "w2"},
				"t2": {"w2", "w3"},
				"t3": {"w3", "w1"},
			},
			want: []TaskID{"t1", "t2", "t3"},
		},
		"some tasks with the same number of workers": {
			input: mapTaskWorkers{
				"t1": {"w3", "w1"},
				"t2": {"w3", "w2"},
				"t3": {"w3", "w2", "w1"},
			},
			want: []TaskID{"t1", "t2", "t3"},
		},
	}
	copts := cmp.Options{}

	for title, tc := range testCases {
		got := tc.input.getSortedTasksByLessWorkers()
		if diff := cmp.Diff(tc.want, got, copts); diff != "" {
			t.Errorf("%s: mismatch (-want +got):\n%v", title, diff)
		}
	}
}
