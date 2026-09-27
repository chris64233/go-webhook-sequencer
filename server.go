package webhooksequencer

import (
	"encoding/json"
	"errors"
	"net/http"
)

// Server 把顺序接收与投递能力暴露为 HTTP 接口：
//
//	PUT    /v1/sources/{id}                初始化来源
//	POST   /v1/sources/{id}/events         接收事件
//	POST   /v1/deliveries/{key}/ack        投递确认（通常由投递器内部调用）
//	POST   /v1/deliveries/{key}/release    主动放弃租约
//	GET    /v1/sources/{id}/backlog        积压查询
//	GET    /v1/sources/{id}                来源信息
//
// 路径参数中的 {key} 形如 "src-1:42"，以最后一个 ':' 之前作为来源标识，
// 因此来源标识本身可以包含 ':'。
type Server struct {
	store Store
	mux   *http.ServeMux
}

// NewServer 构造挂载在指定 Store 上的 HTTP 服务。
func NewServer(store Store) *Server {
	if store == nil {
		panic("webhooksequencer: store is required")
	}
	s := &Server{store: store, mux: http.NewServeMux()}
	s.routes()
	return s
}

// ServeHTTP 实现 http.Handler。
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *Server) routes() {
	s.mux.HandleFunc("PUT /v1/sources/{id}", s.handleInitSource)
	s.mux.HandleFunc("GET /v1/sources/{id}", s.handleGetSource)
	s.mux.HandleFunc("POST /v1/sources/{id}/events", s.handleReceive)
	s.mux.HandleFunc("GET /v1/sources/{id}/backlog", s.handleBacklog)
	s.mux.HandleFunc("POST /v1/deliveries/{key}/ack", s.handleAck)
	s.mux.HandleFunc("POST /v1/deliveries/{key}/release", s.handleRelease)
}

type initSourceRequest struct {
	StartSeq int64 `json:"start_seq"`
}

func (s *Server) handleInitSource(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req initSourceRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	info, err := s.store.InitSource(id, req.StartSeq)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleGetSource(w http.ResponseWriter, r *http.Request) {
	info, err := s.store.GetSource(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

type receiveRequest struct {
	Seq     int64  `json:"seq"`
	ID      string `json:"id"`
	Digest  string `json:"digest"`
	Payload []byte `json:"payload,omitempty"`
}

func (s *Server) handleReceive(w http.ResponseWriter, r *http.Request) {
	var req receiveRequest
	if err := decodeJSON(w, r, &req); err != nil {
		writeError(w, err)
		return
	}
	receipt, err := s.store.Receive(Event{
		SourceID: r.PathValue("id"),
		Seq:      req.Seq,
		ID:       req.ID,
		Digest:   req.Digest,
		Payload:  req.Payload,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, receipt)
}

func (s *Server) handleBacklog(w http.ResponseWriter, r *http.Request) {
	bl, err := s.store.GetBacklog(r.PathValue("id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, bl)
}

type ackRequest struct {
	Attempt int `json:"attempt"`
}

func (s *Server) handleAck(w http.ResponseWriter, r *http.Request) {
	if err := s.completeLease(w, r, true); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleRelease(w http.ResponseWriter, r *http.Request) {
	if err := s.completeLease(w, r, false); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) completeLease(w http.ResponseWriter, r *http.Request, ack bool) error {
	key := r.PathValue("key")
	var req ackRequest
	// body 可空：默认 attempt=1，方便手工/调试调用；正式调用方应回传领取时的 attempt。
	if r.ContentLength != 0 {
		if err := decodeJSON(w, r, &req); err != nil {
			return err
		}
	}
	if req.Attempt <= 0 {
		req.Attempt = 1
	}
	if ack {
		return s.store.Ack(key, req.Attempt)
	}
	return s.store.Release(key, req.Attempt)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, v any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return invalidArg("body", "invalid JSON: "+err.Error())
	}
	return nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// errorResponse 是统一错误响应体。
type errorResponse struct {
	Error  string `json:"error"`
	Kind   string `json:"kind"`
	Field  string `json:"field,omitempty"`
	Detail any    `json:"detail,omitempty"`
}

func writeError(w http.ResponseWriter, err error) {
	resp := errorResponse{Error: err.Error()}
	status := http.StatusInternalServerError

	var iae *InvalidArgumentError
	var ce *ConflictError
	switch {
	case errors.Is(err, ErrInvalidArgument):
		status = http.StatusBadRequest
		resp.Kind = "invalid_argument"
		if errors.As(err, &iae) {
			resp.Field = iae.Field
		}
	case errors.Is(err, ErrNotFound):
		status = http.StatusNotFound
		resp.Kind = "not_found"
	case errors.Is(err, ErrAlreadyExists):
		status = http.StatusConflict
		resp.Kind = "already_exists"
	case errors.Is(err, ErrSequenceConflict):
		status = http.StatusConflict
		resp.Kind = "sequence_conflict"
		if errors.As(err, &ce) {
			resp.Detail = ce
		}
	case errors.Is(err, ErrPayloadConflict):
		status = http.StatusUnprocessableEntity
		resp.Kind = "payload_conflict"
		if errors.As(err, &ce) {
			resp.Detail = ce
		}
	case errors.Is(err, ErrLeaseLost):
		status = http.StatusPreconditionFailed
		resp.Kind = "lease_lost"
	}
	writeJSON(w, status, resp)
}
