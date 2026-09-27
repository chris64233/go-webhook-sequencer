package webhooksequencer

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPEndToEnd(t *testing.T) {
	ms := NewMemoryStore()
	srv := httptest.NewServer(NewServer(ms))
	defer srv.Close()
	c := srv.Client()

	// 1) 初始化来源。
	if r := doJSON(t, c, http.MethodPut, srv.URL+"/v1/sources/src", map[string]any{"start_seq": 1}); r.Code != http.StatusOK {
		t.Fatalf("init source: %s", r.Text)
	}
	// 重复初始化（相同 startSeq）幂等。
	if r := doJSON(t, c, http.MethodPut, srv.URL+"/v1/sources/src", map[string]any{"start_seq": 1}); r.Code != http.StatusOK {
		t.Fatalf("idempotent init: %s", r.Text)
	}
	// 不同 startSeq：409 already_exists。
	if r := doJSON(t, c, http.MethodPut, srv.URL+"/v1/sources/src", map[string]any{"start_seq": 9}); r.Code != http.StatusConflict || r.JSON["kind"] != "already_exists" {
		t.Fatalf("want 409 already_exists, got %s", r.Text)
	}

	// 2) 乱序接收：seq=2 先到 → buffered。
	r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events",
		map[string]any{"seq": 2, "id": "e2", "digest": "d2"})
	if r.Code != http.StatusOK || r.JSON["status"] != string(StatusBuffered) {
		t.Fatalf("want buffered, got %s", r.Text)
	}

	// 未知来源 → 404。
	if r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/nope/events",
		map[string]any{"seq": 1, "id": "x", "digest": "d"}); r.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %s", r.Text)
	}
	// 参数错误 → 400 invalid_argument 带字段名。
	r = doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events",
		map[string]any{"seq": 1, "id": "e1", "digest": ""})
	if r.Code != http.StatusBadRequest || r.JSON["kind"] != "invalid_argument" || r.JSON["field"] != "digest" {
		t.Fatalf("want 400 invalid_argument field=digest, got %s", r.Text)
	}
	// 非法 JSON → 400。
	if r := doRaw(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events", "{not json"); r.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for bad json, got %s", r.Text)
	}

	// 3) 补齐缺口 seq=1 → 一次释放 1、2。
	r = doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events",
		map[string]any{"seq": 1, "id": "e1", "digest": "d1"})
	if r.Code != http.StatusOK || r.JSON["status"] != string(StatusQueued) ||
		r.JSON["released_count"].(float64) != 2 || r.JSON["idempotency_key"] != "src:1" {
		t.Fatalf("want queued release 1-2, got %s", r.Text)
	}

	// 重复提交 seq=2：200 且 Duplicate=true，返回首次 buffered 原始结果。
	r = doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events",
		map[string]any{"seq": 2, "id": "e2", "digest": "d2"})
	if r.Code != http.StatusOK || r.JSON["duplicate"] != true || r.JSON["status"] != string(StatusBuffered) {
		t.Fatalf("duplicate should return original buffered receipt, got %s", r.Text)
	}

	// 序号冲突：同序号不同标识 → 409 sequence_conflict。
	r = doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events",
		map[string]any{"seq": 1, "id": "other", "digest": "dx"})
	if r.Code != http.StatusConflict || r.JSON["kind"] != "sequence_conflict" {
		t.Fatalf("want 409 sequence_conflict, got %s", r.Text)
	}
	// 负载冲突：同标识不同摘要 → 422 payload_conflict。
	r = doJSON(t, c, http.MethodPost, srv.URL+"/v1/sources/src/events",
		map[string]any{"seq": 1, "id": "e1", "digest": "tampered"})
	if r.Code != http.StatusUnprocessableEntity || r.JSON["kind"] != "payload_conflict" {
		t.Fatalf("want 422 payload_conflict, got %s", r.Text)
	}

	// 4) 积压查询：pending=2（1、2 已释放未确认）。
	r = doJSON(t, c, http.MethodGet, srv.URL+"/v1/sources/src/backlog", nil)
	if r.Code != http.StatusOK || r.JSON["pending_delivery"].(float64) != 2 || r.JSON["buffered"].(float64) != 0 {
		t.Fatalf("unexpected backlog: %s", r.Text)
	}

	// 5) 来源信息查询。
	r = doJSON(t, c, http.MethodGet, srv.URL+"/v1/sources/src", nil)
	if r.Code != http.StatusOK || r.JSON["next_seq"].(float64) != 3 {
		t.Fatalf("unexpected source info: %s", r.Text)
	}
}

func TestHTTPAckLifecycle(t *testing.T) {
	ms := NewMemoryStore()
	srv := httptest.NewServer(NewServer(ms))
	defer srv.Close()
	c := srv.Client()

	if _, err := ms.InitSource("s", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := ms.Receive(mkEvent("s", 1, "e1", "d1")); err != nil {
		t.Fatal(err)
	}
	due := ms.ClaimDue(time.Now(), time.Minute, 10)
	if len(due) != 1 {
		t.Fatalf("want one due, got %d", len(due))
	}

	// 未知 key → 404。
	if r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/deliveries/s:99/ack", map[string]any{"attempt": 1}); r.Code != http.StatusNotFound {
		t.Fatalf("want 404 unknown delivery, got %s", r.Text)
	}
	// 错误 attempt（fencing）→ 412 lease_lost。
	r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/deliveries/s:1/ack", map[string]any{"attempt": due[0].Attempt + 5})
	if r.Code != http.StatusPreconditionFailed || r.JSON["kind"] != "lease_lost" {
		t.Fatalf("want 412 lease_lost, got %s", r.Text)
	}
	// 正确 attempt 确认 → 204。
	if r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/deliveries/s:1/ack", map[string]any{"attempt": due[0].Attempt}); r.Code != http.StatusNoContent {
		t.Fatalf("want 204, got %s", r.Text)
	}
	// 重复确认终态 → 204 幂等。
	if r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/deliveries/s:1/ack", map[string]any{"attempt": due[0].Attempt}); r.Code != http.StatusNoContent {
		t.Fatalf("duplicate ack should be 204, got %s", r.Text)
	}

	// release 未知来源（key 合法但来源不存在）→ 404；畸形 key → 400。
	if r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/deliveries/missing:1/release", map[string]any{"attempt": 1}); r.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %s", r.Text)
	}
	if r := doJSON(t, c, http.MethodPost, srv.URL+"/v1/deliveries/malformed/release", map[string]any{"attempt": 1}); r.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for malformed key, got %s", r.Text)
	}
}

// ---- test helpers ----

type apiResp struct {
	Code int
	JSON map[string]any
	Text string
}

func doJSON(t *testing.T, c *http.Client, method, url string, body any) apiResp {
	t.Helper()
	var buf bytes.Buffer
	if body != nil {
		if err := json.NewEncoder(&buf).Encode(body); err != nil {
			t.Fatal(err)
		}
	}
	req, err := http.NewRequest(method, url, &buf)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return finish(t, c, req)
}

func doRaw(t *testing.T, c *http.Client, method, url, body string) apiResp {
	t.Helper()
	req, err := http.NewRequest(method, url, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	return finish(t, c, req)
}

func finish(t *testing.T, c *http.Client, req *http.Request) apiResp {
	t.Helper()
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	out := apiResp{Code: resp.StatusCode, Text: "status " + resp.Status + " body " + string(data)}
	_ = json.Unmarshal(data, &out.JSON)
	return out
}
