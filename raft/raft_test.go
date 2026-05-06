package raft

import (
	"bytes"
	"encoding/gob"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
)

func TestElectionBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, orgTerm := h.CheckSingleLeader()

	h.DisconnectPeer(orgLeaderID)
	sleepMs(350)

	newLeaderID, newTerm := h.CheckSingleLeader()
	if newLeaderID == orgLeaderID {
		t.Errorf("want new leader to be different from org leader")
	}
	if newTerm <= orgTerm {
		t.Errorf("want newTerm <= orgTerm, got %d and %d", newTerm, orgTerm)
	}
}

func TestElectionLeaderAndAnotherDisconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()

	h.DisconnectPeer(orgLeaderID)
	otherID := (orgLeaderID + 1) % 3
	h.DisconnectPeer(otherID)

	sleepMs(450)
	h.CheckNoLeader()

	h.ReconnectPeer(otherID)
	h.CheckSingleLeader()
}

func TestDisconnectAllThenRestore(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	sleepMs(100)

	for i := 0; i < 3; i++ {
		h.DisconnectPeer(i)
	}
	sleepMs(450)
	h.CheckNoLeader()

	for i := 0; i < 3; i++ {
		h.ReconnectPeer(i)
	}
	h.CheckSingleLeader()
}

func TestElectionLeaderDisconnectThenReconnect(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()

	h.DisconnectPeer(orgLeaderID)

	sleepMs(350)
	newLeaderID, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(orgLeaderID)
	sleepMs(150)

	againLeaderID, againTerm := h.CheckSingleLeader()

	if newLeaderID != againLeaderID {
		t.Errorf("again leader id got %d; want %d", againLeaderID, newLeaderID)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionLeaderDisconnectThenReconnect5(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()

	h.DisconnectPeer(orgLeaderID)
	sleepMs(150)
	newLeaderID, newTerm := h.CheckSingleLeader()

	h.ReconnectPeer(orgLeaderID)
	sleepMs(150)

	againLeaderID, againTerm := h.CheckSingleLeader()

	if newLeaderID != againLeaderID {
		t.Errorf("again leader id got %d; want %d", againLeaderID, newLeaderID)
	}
	if againTerm != newTerm {
		t.Errorf("again term got %d; want %d", againTerm, newTerm)
	}
}

func TestElectionFollowerComesBack(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, orgTerm := h.CheckSingleLeader()

	otherID := (orgLeaderID + 1) % 3
	h.DisconnectPeer(otherID)
	sleepMs(650)
	h.ReconnectPeer(otherID)
	sleepMs(150)

	_, newTerm := h.CheckSingleLeader()
	if newTerm <= orgTerm {
		t.Errorf("newTerm=%d, orgTerm=%d", newTerm, orgTerm)
	}
}

func TestElectionDisconnectLoop(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	for cycle := 0; cycle < 5; cycle++ {
		leaderID, _ := h.CheckSingleLeader()

		h.DisconnectPeer(leaderID)
		otherID := (leaderID + 1) % 3
		h.DisconnectPeer(otherID)
		sleepMs(310)
		h.CheckNoLeader()

		h.ReconnectPeer(otherID)
		h.ReconnectPeer(leaderID)

		sleepMs(150)
	}
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

func TestCommitAfterCallDrops(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderID, _ := h.CheckSingleLeader()
	h.PeerDropCallsAfterN(leaderID, 2)
	h.SubmitToServer(leaderID, 99)
	sleepMs(30)
	h.PeerDontDropCalls(leaderID)

	sleepMs(60)
	h.CheckCommittedN(99, 3)
}

func TestSubmitNonLeaderFails(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()
	sid := (orgLeaderID + 1) % 3
	tlog("submitting 42 to %d", sid)
	isLeader := h.SubmitToServer(sid, 42) >= 0
	if isLeader {
		t.Errorf("want id=%d !leader, but it is", sid)
	}
	sleepMs(10)
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

func TestCommitWithDisconnectionAndRecover(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderID, 5)
	h.SubmitToServer(orgLeaderID, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	dPeerID := (orgLeaderID + 1) % 3
	h.DisconnectPeer(dPeerID)
	sleepMs(250)

	h.SubmitToServer(orgLeaderID, 7)
	sleepMs(250)
	h.CheckCommittedN(7, 2)

	h.ReconnectPeer(dPeerID)
	sleepMs(250)
	h.CheckSingleLeader()

	sleepMs(150)
	h.CheckCommittedN(7, 3)
}

func TestNoCommitWithNoQuorum(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderID, orgTerm := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderID, 5)
	h.SubmitToServer(orgLeaderID, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	dPeerID1 := (orgLeaderID + 1) % 3
	dPeerID2 := (orgLeaderID + 2) % 3
	h.DisconnectPeer(dPeerID1)
	h.DisconnectPeer(dPeerID2)
	sleepMs(250)

	h.SubmitToServer(orgLeaderID, 8)
	sleepMs(250)
	h.CheckNotCommitted(8)

	h.ReconnectPeer(dPeerID1)
	h.ReconnectPeer(dPeerID2)
	sleepMs(600)

	h.CheckNotCommitted(8)

	newLeaderID, againTerm := h.CheckSingleLeader()
	if orgTerm == againTerm {
		t.Errorf("got orgTerm==againTerm==%d; want them different", orgTerm)
	}

	h.SubmitToServer(newLeaderID, 9)
	h.SubmitToServer(newLeaderID, 10)
	h.SubmitToServer(newLeaderID, 11)
	sleepMs(350)

	for _, v := range []int{9, 10, 11} {
		h.CheckCommittedN(v, 3)
	}
}

func TestDisconnectLeaderBriefly(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderId, 5)
	h.SubmitToServer(orgLeaderId, 6)
	sleepMs(250)
	h.CheckCommittedN(6, 3)

	h.DisconnectPeer(orgLeaderId)
	sleepMs(90)
	h.ReconnectPeer(orgLeaderId)
	sleepMs(200)

	h.SubmitToServer(orgLeaderId, 7)
	sleepMs(250)
	h.CheckCommittedN(7, 3)
}

func TestCommitsWithLeaderDisconnects(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 5)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderId, 5)
	h.SubmitToServer(orgLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 5)

	h.DisconnectPeer(orgLeaderId)
	sleepMs(10)

	h.SubmitToServer(orgLeaderId, 7)

	sleepMs(250)
	h.CheckNotCommitted(7)

	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, 8)
	sleepMs(250)
	h.CheckCommittedN(8, 4)

	h.ReconnectPeer(orgLeaderId)
	sleepMs(600)

	finalLeaderId, _ := h.CheckSingleLeader()
	if finalLeaderId == orgLeaderId {
		t.Errorf("got finalLeaderId==origLeaderId==%d, want them different", finalLeaderId)
	}

	h.SubmitToServer(newLeaderId, 9)
	sleepMs(250)
	h.CheckCommittedN(9, 5)
	h.CheckCommittedN(8, 5)

	h.CheckNotCommitted(7)
}

func TestCrashFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	oriLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(oriLeaderId, 5)

	sleepMs(350)
	h.CheckCommittedN(5, 3)

	h.CrashPeer((oriLeaderId + 1) % 3)
	sleepMs(350)
	h.CheckCommittedN(5, 2)
}

func TestCrashThenRestartFollower(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderId, 5)
	h.SubmitToServer(orgLeaderId, 6)
	h.SubmitToServer(orgLeaderId, 7)

	vals := []int{5, 6, 7}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	h.CrashPeer((orgLeaderId + 1) % 3)
	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}

	h.RestartPeer((orgLeaderId + 1) % 3)
	sleepMs(650)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

func TestCrashThenRestartLeader(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderId, 5)
	h.SubmitToServer(orgLeaderId, 6)
	h.SubmitToServer(orgLeaderId, 7)

	vals := []int{5, 6, 7}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	h.CrashPeer(orgLeaderId)
	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 2)
	}

	h.RestartPeer(orgLeaderId)
	sleepMs(550)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

func TestCrashThenRestartAll(t *testing.T) {
	defer leaktest.CheckTimeout(t, 100*time.Millisecond)()

	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderId, 5)
	h.SubmitToServer(orgLeaderId, 6)
	h.SubmitToServer(orgLeaderId, 7)

	vals := []int{5, 6, 7}

	sleepMs(350)
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}

	for i := 0; i < 3; i++ {
		h.CrashPeer((orgLeaderId + i) % 3)
	}

	sleepMs(350)

	for i := 0; i < 3; i++ {
		h.RestartPeer((orgLeaderId + i) % 3)
	}

	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, 8)
	sleepMs(250)

	vals = []int{5, 6, 7, 8}
	for _, v := range vals {
		h.CheckCommittedN(v, 3)
	}
}

func TestReplaceMultipleLogEntries(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()
	h.SubmitToServer(orgLeaderId, 5)
	h.SubmitToServer(orgLeaderId, 6)

	sleepMs(250)
	h.CheckCommittedN(6, 3)

	h.DisconnectPeer(orgLeaderId)
	sleepMs(10)

	h.SubmitToServer(orgLeaderId, 21)
	sleepMs(5)
	h.SubmitToServer(orgLeaderId, 22)
	sleepMs(5)
	h.SubmitToServer(orgLeaderId, 23)
	sleepMs(5)
	h.SubmitToServer(orgLeaderId, 24)
	sleepMs(5)

	newLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(newLeaderId, 8)
	sleepMs(5)
	h.SubmitToServer(newLeaderId, 9)
	sleepMs(5)
	h.SubmitToServer(newLeaderId, 10)
	sleepMs(250)
	h.CheckNotCommitted(21)
	h.CheckCommittedN(10, 2)

	h.CrashPeer(newLeaderId)
	sleepMs(60)
	h.RestartPeer(newLeaderId)

	sleepMs(100)
	finalLeaderId, _ := h.CheckSingleLeader()
	h.ReconnectPeer(orgLeaderId)
	sleepMs(400)

	h.SubmitToServer(finalLeaderId, 11)
	sleepMs(250)

	h.CheckNotCommitted(21)
	h.CheckCommittedN(11, 3)
	h.CheckCommittedN(10, 3)
}

func TestCrashAfterSubmit(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(orgLeaderId, 5)
	sleepMs(1)
	h.CrashPeer(orgLeaderId)

	sleepMs(10)
	h.CheckSingleLeader()
	sleepMs(300)
	h.CheckNotCommitted(5)

	h.RestartPeer(orgLeaderId)
	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()
	h.CheckNotCommitted(5)

	h.SubmitToServer(newLeaderId, 6)
	sleepMs(100)
	h.CheckCommittedN(5, 3)
	h.CheckCommittedN(6, 3)
}

func TestDisconnectAfterSubmit(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, _ := h.CheckSingleLeader()

	h.SubmitToServer(orgLeaderId, 5)
	sleepMs(1)
	h.DisconnectPeer(orgLeaderId)

	sleepMs(10)
	h.CheckSingleLeader()
	sleepMs(300)
	h.CheckNotCommitted(5)

	h.ReconnectPeer(orgLeaderId)
	sleepMs(150)
	newLeaderId, _ := h.CheckSingleLeader()
	h.CheckNotCommitted(5)

	h.SubmitToServer(newLeaderId, 6)
	sleepMs(100)
	h.CheckCommittedN(5, 3)
	h.CheckCommittedN(6, 3)
}

func getPersistedTerm(storage *MapStorage) int {
	data, found := storage.Get("currentTerm")
	if !found {
		return 0
	}
	var term int
	d := gob.NewDecoder(bytes.NewBuffer(data))
	if err := d.Decode(&term); err != nil {
		return 0
	}
	return term
}

func TestBug_StartElectionMissingPersist(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderID, _ := h.CheckSingleLeader()
	victim := (leaderID + 1) % 3

	h.DisconnectPeer(victim)

	time.Sleep(1200 * time.Millisecond)

	cm := h.cluster[victim].cm
	cm.mu.Lock()
	inMemoryTerm := cm.currentTerm
	cm.mu.Unlock()

	persistedTerm := getPersistedTerm(h.storage[victim])

	t.Logf("server %d: in-memory term = %d, persisted term = %d", victim, inMemoryTerm, persistedTerm)

	if persistedTerm < inMemoryTerm {
		t.Errorf("persisted term (%d) is behind in-memory term (%d); "+
			"startElection is not persisting state", persistedTerm, inMemoryTerm)
	}
}

func TestBug_BecomeFollowerMissingPersist(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	orgLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(orgLeaderId)
	sleepMs(350)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Fatalf("got newTerm=%d, origTerm=%d; want newTerm > origTerm", newTerm, origTerm)
	}

	h.PeerDropCallsAfterN(newLeaderId, 0)
	defer h.PeerDontDropCalls(newLeaderId)

	h.ReconnectPeer(orgLeaderId)
	sleepMs(120)

	_, steppedDownTerm, isLeader := h.cluster[orgLeaderId].cm.Report()
	if isLeader {
		t.Fatalf("server %d still thinks it's leader after reconnect", orgLeaderId)
	}
	if steppedDownTerm != newTerm {
		t.Fatalf("server %d has term %d after step-down; want %d", orgLeaderId, steppedDownTerm, newTerm)
	}

	h.CrashPeer(orgLeaderId)
	h.RestartPeer(orgLeaderId)

	_, restartedTerm, _ := h.cluster[orgLeaderId].cm.Report()
	if restartedTerm != newTerm {
		t.Fatalf("server %d restarted with term %d; want persisted higher term %d", orgLeaderId, restartedTerm, newTerm)
	}
}

func TestBecomeFollowerSameTermPreservesVotedFor(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()

	for i := 0; i < 3; i++ {
		cm := h.cluster[i].cm
		cm.mu.Lock()
		if cm.state == Follower && cm.votedFor >= 0 {
			savedVotedFor := cm.votedFor
			savedTerm := cm.currentTerm

			cm.becomeFollower(savedTerm)

			if cm.votedFor != savedVotedFor {
				t.Errorf("becomeFollower(%d) reset votedFor from %d to %d on same-term transition",
					savedTerm, savedVotedFor, cm.votedFor)
			}
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
	t.Fatal("no follower with votedFor >= 0 found")
}

func TestBecomeFollowerHigherTermResetsVotedFor(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	h.CheckSingleLeader()

	for i := 0; i < 3; i++ {
		cm := h.cluster[i].cm
		cm.mu.Lock()
		if cm.state == Follower && cm.votedFor >= 0 {
			savedTerm := cm.currentTerm

			cm.becomeFollower(savedTerm + 1)

			if cm.votedFor != -1 {
				t.Errorf("becomeFollower(%d) did not reset votedFor (got %d, want -1)",
					savedTerm+1, cm.votedFor)
			}
			cm.mu.Unlock()
			return
		}
		cm.mu.Unlock()
	}
	t.Fatal("no follower with votedFor >= 0 found")
}

func TestStaleVoteReplyIgnored(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	orgLeaderId, origTerm := h.CheckSingleLeader()

	h.DisconnectPeer(orgLeaderId)
	sleepMs(450)

	newLeaderId, newTerm := h.CheckSingleLeader()
	if newTerm <= origTerm {
		t.Fatalf("expected newTerm > origTerm, got %d <= %d", newTerm, origTerm)
	}

	h.DisconnectPeer(newLeaderId)
	sleepMs(450)

	h.ReconnectPeer(orgLeaderId)
	h.ReconnectPeer(newLeaderId)
	sleepMs(450)

	h.CheckSingleLeader()
}

func TestSameTermDoubleVotePrevented(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()

	leaderId, leaderTerm := h.CheckSingleLeader()

	followerId := -1
	for i := 0; i < 3; i++ {
		if i == leaderId {
			continue
		}
		cm := h.cluster[i].cm
		cm.mu.Lock()
		if cm.votedFor == leaderId && cm.currentTerm == leaderTerm {
			followerId = i
		}
		cm.mu.Unlock()
		if followerId >= 0 {
			break
		}
	}
	if followerId < 0 {
		t.Fatal("could not find a follower that voted for the leader")
	}

	otherCandidate := -1
	for i := 0; i < 3; i++ {
		if i != leaderId && i != followerId {
			otherCandidate = i
			break
		}
	}

	cm := h.cluster[followerId].cm
	args := RequestVoteArgs{
		Term:         leaderTerm,
		CandidateID:  otherCandidate,
		LastLogIndex: -1,
		LastLogTerm:  -1,
	}
	var reply RequestVoteReply
	if err := cm.RequestVote(args, &reply); err != nil {
		t.Fatal(err)
	}

	if reply.VoteGranted {
		t.Errorf("follower %d granted vote to %d in term %d, but already voted for %d",
			followerId, otherCandidate, leaderTerm, leaderId)
	}
}

func TestElectionSafetyStress(t *testing.T) {
	h := NewHarness(t, 5)
	defer h.Shutdown()

	for cycle := 0; cycle < 8; cycle++ {
		leaderId, _ := h.CheckSingleLeader()
		h.DisconnectPeer(leaderId)
		sleepMs(350)

		h.CheckSingleLeader()

		h.ReconnectPeer(leaderId)
		sleepMs(150)
	}

	time.Sleep(300 * time.Millisecond)
	h.CheckSingleLeader()
}
