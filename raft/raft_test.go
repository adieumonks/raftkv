package raft

import (
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

func TestCommitOneCommand(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()

	tlog("submitting 42 to %d", orgLeaderID)
	isLeader := h.SubmitToServer(orgLeaderID, 42) >= 0
	if !isLeader {
		t.Errorf("want id=%d leader, but it's not", orgLeaderID)
	}

	sleepMs(250)
	h.CheckCommittedN(42, 3)
}

func TestCommitMultipleCommands(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()

	values := []int{42, 55, 81}
	for _, v := range values {
		tlog("submitting %d to %d", v, orgLeaderID)
		isLeader := h.SubmitToServer(orgLeaderID, v) >= 0
		if !isLeader {
			t.Errorf("want id=%d leader, but it's not", orgLeaderID)
		}
		sleepMs(100)
	}

	sleepMs(250)
	nc, i1 := h.CheckCommitted(42)
	_, i2 := h.CheckCommitted(55)
	if nc != 3 {
		t.Errorf("want nc=3, got %d", nc)
	}
	if i1 >= i2 {
		t.Errorf("want i1<i2, got i1=%d i2=%d", i1, i2)
	}

	_, i3 := h.CheckCommitted(81)
	if i2 >= i3 {
		t.Errorf("want i2<i3, got i2=%d i3=%d", i2, i3)
	}
}
