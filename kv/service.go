package kv

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/adieumonks/raftkv/api"
	"github.com/adieumonks/raftkv/raft"
)

const DebugKV = 0

type Service struct {
	sync.Mutex

	id int

	rs *raft.Server

	commitChan chan raft.CommitEntry

	commitSubs map[int]chan Command

	ds *DataStore

	srv *http.Server

	lastRequestIDPerClient map[int64]int64

	delayNextHTTPResponse atomic.Bool
}

func NewService(id int, peerIDs []int, storage raft.Storage, readyChan <-chan any) *Service {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	rs := raft.NewServer(id, peerIDs, storage, readyChan, commitChan)
	rs.Serve()
	s := &Service{
		id:                     id,
		rs:                     rs,
		commitChan:             commitChan,
		ds:                     NewDataStore(),
		commitSubs:             make(map[int]chan Command),
		lastRequestIDPerClient: make(map[int64]int64),
	}

	s.runUpdater()
	return s
}

func (s *Service) IsLeader() bool {
	return s.rs.IsLeader()
}

func (s *Service) ServeHTTP(port int) {
	if s.srv != nil {
		panic("ServeHTTP called with existing server")
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", s.handleGet)
	mux.HandleFunc("POST /put/", s.handlePut)
	mux.HandleFunc("POST /append/", s.handleAppend)
	mux.HandleFunc("POST /cas/", s.handleCAS)

	s.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	srv := s.srv

	go func() {
		s.log("serving HTTP on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal(err)
		}
	}()
}

func (s *Service) ShutDown() error {
	s.log("shutting down Raft server")
	s.rs.Shutdown()
	s.log("closing commitChan")
	close(s.commitChan)

	if s.srv != nil {
		s.log("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		if err := s.srv.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		s.log("HTTP shutdown complete")
		return nil
	}

	return nil
}

func (s *Service) DelayNextHTTPResponse() {
	s.delayNextHTTPResponse.Store(true)
}

func (s *Service) sendHTTPResponse(w http.ResponseWriter, v any) {
	if s.delayNextHTTPResponse.Load() {
		s.delayNextHTTPResponse.Store(false)
		time.Sleep(300 * time.Millisecond)
	}
	s.log("sending response %#v", v)
	renderJSON(w, v)
}

func (s *Service) handlePut(w http.ResponseWriter, req *http.Request) {
	pr := &api.PutRequest{}
	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.log("HTTP PUT %v", pr)

	cmd := Command{
		Kind:      CommandPut,
		Key:       pr.Key,
		Value:     pr.Value,
		ServiceID: s.id,
		ClientID:  pr.ClientID,
		RequestID: pr.RequestID,
	}
	logIndex := s.rs.Submit(cmd)
	if logIndex < 0 {
		s.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := s.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == s.id {
			if commitCmd.IsDuplicate {
				s.sendHTTPResponse(w, api.PutResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				s.sendHTTPResponse(w, api.PutResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			s.sendHTTPResponse(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (s *Service) handleAppend(w http.ResponseWriter, req *http.Request) {
	ar := &api.AppendRequest{}
	if err := readRequestJSON(req, ar); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.log("HTTP APPEND %v", ar)

	cmd := Command{
		Kind:      CommandAppend,
		Key:       ar.Key,
		Value:     ar.Value,
		ServiceID: s.id,
		ClientID:  ar.ClientID,
		RequestID: ar.RequestID,
	}
	logIndex := s.rs.Submit(cmd)
	if logIndex < 0 {
		s.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := s.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == s.id {
			if commitCmd.IsDuplicate {
				s.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				s.sendHTTPResponse(w, api.AppendResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			s.sendHTTPResponse(w, api.AppendResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (s *Service) handleGet(w http.ResponseWriter, req *http.Request) {
	gr := &api.GetRequest{}
	if err := readRequestJSON(req, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.log("HTTP GET %v", gr)

	cmd := Command{
		Kind:      CommandGet,
		Key:       gr.Key,
		ServiceID: s.id,
		ClientID:  gr.ClientID,
		RequestID: gr.RequestID,
	}
	logIndex := s.rs.Submit(cmd)
	if logIndex < 0 {
		s.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := s.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == s.id {
			if commitCmd.IsDuplicate {
				s.sendHTTPResponse(w, api.GetResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				s.sendHTTPResponse(w, api.GetResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					Value:      commitCmd.ResultValue,
				})
			}
		} else {
			s.sendHTTPResponse(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (s *Service) handleCAS(w http.ResponseWriter, req *http.Request) {
	cr := &api.CASRequest{}
	if err := readRequestJSON(req, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.log("HTTP CAS %v", cr)

	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		Value:        cr.Value,
		CompareValue: cr.CompareValue,
		ServiceID:    s.id,
		ClientID:     cr.ClientID,
		RequestID:    cr.RequestID,
	}
	logIndex := s.rs.Submit(cmd)
	if logIndex < 0 {
		s.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := s.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.ServiceID == s.id {
			if commitCmd.IsDuplicate {
				s.sendHTTPResponse(w, api.CASResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				s.sendHTTPResponse(w, api.CASResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			s.sendHTTPResponse(w, api.CASResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (s *Service) runUpdater() {
	go func() {
		for entry := range s.commitChan {
			cmd := entry.Command.(Command)

			lastReqID, ok := s.lastRequestIDPerClient[cmd.ClientID]
			if ok && lastReqID >= cmd.RequestID {
				s.log("duplicate request id=%v, from client id=%v", cmd.RequestID, cmd.ClientID)
				cmd = Command{
					Kind:        cmd.Kind,
					IsDuplicate: true,
				}
			} else {
				s.lastRequestIDPerClient[cmd.ClientID] = cmd.RequestID

				switch cmd.Kind {
				case CommandGet:
					cmd.ResultValue, cmd.ResultFound = s.ds.Get(cmd.Key)
				case CommandPut:
					cmd.ResultValue, cmd.ResultFound = s.ds.Put(cmd.Key, cmd.Value)
				case CommandAppend:
					cmd.ResultValue, cmd.ResultFound = s.ds.Append(cmd.Key, cmd.Value)
				case CommandCAS:
					cmd.ResultValue, cmd.ResultFound = s.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
				default:
					panic(fmt.Errorf("unexpected command %v", cmd))
				}
			}

			if sub := s.popCommitSubscription(entry.Index); sub != nil {
				sub <- cmd
				close(sub)
			}
		}
	}()
}

func (s *Service) createCommitSubscription(logIndex int) chan Command {
	s.Lock()
	defer s.Unlock()

	if _, exists := s.commitSubs[logIndex]; exists {
		panic(fmt.Sprintf("duplicate commit subscription for logIndex=%d", logIndex))
	}

	ch := make(chan Command, 1)
	s.commitSubs[logIndex] = ch
	return ch
}

func (s *Service) popCommitSubscription(logIndex int) chan Command {
	s.Lock()
	defer s.Unlock()

	ch := s.commitSubs[logIndex]
	delete(s.commitSubs, logIndex)
	return ch
}

func (s *Service) log(format string, args ...any) {
	if DebugKV > 0 {
		format = fmt.Sprintf("[kv %d] ", s.id) + format
		log.Printf(format, args...)
	}
}

func (s *Service) ConnectToRaftPeer(peerID int, addr net.Addr) error {
	return s.rs.ConnectToPeer(peerID, addr)
}

func (s *Service) DisconnectFromAllRaftPeers() {
	s.rs.DisconnectAll()
}

func (s *Service) DisconnectFromRaftPeer(peerID int) error {
	return s.rs.DisconnectPeer(peerID)
}

func (s *Service) GetRaftListenAddr() net.Addr {
	return s.rs.GetListenAddr()
}
