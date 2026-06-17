package http

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-martini/martini"
	"github.com/martini-contrib/auth"
	"github.com/martini-contrib/render"
	"github.com/openark/orchestrator/go/config"
	"github.com/openark/orchestrator/go/db"
	"github.com/openark/orchestrator/go/inst"
)

func TestForgetClusterReportsDeleteError(t *testing.T) {
	previous := *config.Config
	defer func() { *config.Config = previous }()
	config.Config.BackendDB = "sqlite3"
	config.Config.SQLite3DataFile = filepath.Join(t.TempDir(), "forget-api.db")
	config.Config.SkipOrchestratorDatabaseUpdate = false
	config.Config.AuthenticationMethod = ""
	config.Config.ReadOnly = false
	backend, err := db.OpenOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()
	instance := &inst.Instance{Key: inst.InstanceKey{Hostname: "forget-api", Port: 3306}, ClusterName: "forget-api:3306", Version: "5.7.0"}
	if err := inst.WriteInstance(instance, true, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Exec(`create trigger reject_forget before delete on database_instance begin select raise(abort, 'delete rejected by test'); end`); err != nil {
		t.Fatal(err)
	}
	m := martini.New()
	m.Use(render.Renderer())
	m.Use(func(c martini.Context) { c.Map(auth.User("")) })
	router := martini.NewRouter()
	api := HttpAPI{}
	router.Get("/api/forget-cluster/:clusterHint", api.ForgetCluster)
	m.Action(router.Handle)

	request := func() *httptest.ResponseRecorder {
		t.Helper()
		response := httptest.NewRecorder()
		m.ServeHTTP(response, httptest.NewRequest("GET", "/api/forget-cluster/forget-api:3306", nil))
		return response
	}
	response := request()
	var result struct{ Code, Message string }
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusInternalServerError || result.Code != "ERROR" || !strings.Contains(result.Message, "delete rejected by test") {
		t.Fatalf("delete error was not reported: status=%d body=%s", response.Code, response.Body.String())
	}
	var count int
	if err := backend.QueryRow(`select count(*) from database_instance`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("failed deletion should retain the instance: count=%d error=%v", count, err)
	}
	if _, err := backend.Exec(`drop trigger reject_forget`); err != nil {
		t.Fatal(err)
	}
	response = request()
	if err := json.Unmarshal(response.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || result.Code != "OK" {
		t.Fatalf("successful deletion was not reported: status=%d body=%s", response.Code, response.Body.String())
	}
	if err := backend.QueryRow(`select count(*) from database_instance`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("successful deletion should remove the instance: count=%d error=%v", count, err)
	}
}
