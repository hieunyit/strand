package web

import (
	"net/http"
	"net/url"
	"strings"
	"testing"

	"strand/internal/web/store"
)

func TestPipelineTagsSaveAndPermissions(t *testing.T) {
	e := newCPEnv(t)
	admin := e.admin()
	viewer := e.roleClient(store.RoleViewer)
	for _, client := range []*apiClient{admin, viewer} {
		client.hdr["Content-Type"] = "application/x-www-form-urlencoded"
	}
	form := url.Values{"tags": {"Prod, payments, prod"}}.Encode()
	viewer.must("POST", "/pipelines/orders/tags", form, http.StatusForbidden)
	admin.must("POST", "/pipelines/orders/tags", form, http.StatusSeeOther)

	row, found, err := e.st.GetPipeline(t.Context(), "orders")
	if err != nil || !found || row.Tags != "payments prod" {
		t.Fatalf("stored pipeline tags = %q, found=%v, err=%v", row.Tags, found, err)
	}
	_, page := admin.do("GET", "/pipelines", nil)
	if !strings.Contains(page, `data-tags="payments prod"`) {
		t.Fatal("pipeline list does not expose the stored tags for filtering")
	}
	errorPage := admin.must("POST", "/pipelines/orders/tags", "tags=%21invalid", http.StatusBadRequest)
	if !strings.Contains(errorPage, "Use letters, digits") {
		t.Fatal("tag validation message is missing")
	}
	admin.must("POST", "/pipelines/orders/tags", "tags=", http.StatusSeeOther)
	_, page = admin.do("GET", "/pipelines", nil)
	if strings.Contains(page, `data-tags="payments prod"`) {
		t.Fatal("cleared tags still appear in the pipeline list")
	}
}
