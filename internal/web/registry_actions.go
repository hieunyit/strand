package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"strand/internal/web/store"
)

// Thao tác trên registry DB (chế độ DB): khôi phục file của pipeline bị đánh dấu missing, gỡ pipeline khỏi
// registry, nhận/bỏ qua bản sửa trên đĩa đã bị đặt sang bên. Đi qua ActionRegistry nên có audit trước/sau.
func (s *Server) registerRegistryOps() {
	needDB := func() error {
		return nil
	}
	s.actions.Register(Action{Name: "registry-restore", Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		if err := needDB(); err != nil {
			return ActionResult{}, err
		}
		p := req.Pipeline
		srv.materializeOpt(p.Name, true)
		if !fileExists(p.ConfigPath) {
			return ActionResult{}, &ActionError{Status: http.StatusBadGateway, Msg: "could not write " + p.ConfigPath + " (see the web log)"}
		}
		dctx, cancel := dbCtx()
		defer cancel()
		row, ok, err := srv.db.st.GetPipeline(dctx, p.Name)
		if err != nil || !ok {
			return ActionResult{}, fmt.Errorf("read pipeline: %v", err)
		}
		row.MissingSince = time.Time{}
		if err := srv.db.st.UpdatePipelineMeta(dctx, row); err != nil {
			return ActionResult{}, err
		}
		srv.reg.Refresh(ctx)
		msg := "Config file restored from the database: " + p.ConfigPath
		if q := srv.reg.Get(p.Name); q != nil && q.Missing != "" {
			msg += ". Note: " + q.Missing + " (add it back to web.yaml or the scan globs, or remove the pipeline from the registry)"
		}
		return ActionResult{Message: msg}, nil
	}})
	s.actions.Register(Action{Name: "registry-forget", ConfirmName: true, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		if err := needDB(); err != nil {
			return ActionResult{}, err
		}
		p := req.Pipeline
		if srv.reg.Runtime(ctx, p).Running {
			return ActionResult{}, Conflict("pipeline %s is running: stop it first", p.Name)
		}
		dctx, cancel := dbCtx()
		defer cancel()
		if err := srv.db.st.DeletePipeline(dctx, p.Name); err != nil {
			return ActionResult{}, &ActionError{Status: http.StatusServiceUnavailable, Msg: "metadata database: " + err.Error()}
		}
		srv.reg.Refresh(ctx)
		msg := "Removed " + p.Name + " from the registry (its files were left on disk)."
		if srv.reg.Get(p.Name) != nil {
			msg += " It is still declared in web.yaml or matched by a scan glob, so it was registered again from the file on disk."
		}
		return ActionResult{Message: msg}, nil
	}})
	conflictOp := func(adopt bool) func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
		return func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			if err := needDB(); err != nil {
				return ActionResult{}, err
			}
			p := req.Pipeline
			path := req.Args["path"]
			dctx, cancel := dbCtx()
			defer cancel()
			row, ok, err := srv.db.st.GetPipeline(dctx, p.Name)
			if err != nil || !ok {
				return ActionResult{}, fmt.Errorf("read pipeline: %v", err)
			}
			all := parseConflicts(row.Conflicts)
			idx := -1
			for i, c := range all {
				if c.Path == path {
					idx = i
				}
			}
			if idx < 0 {
				return ActionResult{}, BadRequest("no such set-aside file for this pipeline")
			}
			c := all[idx]
			msg := "Dismissed; the set-aside file " + c.Path + " is kept on disk."
			if adopt {
				b, err := os.ReadFile(c.Path)
				if err != nil {
					return ActionResult{}, BadRequest("read %s: %v", c.Path, err)
				}
				switch c.File {
				case "config":
					// Đường sửa cấu hình thường: validate, .bak, ghi file + DB (thất bại thì không đổi gì).
					if _, err := srv.saveConfigContent(ctx, p, string(b), false, false, req.IP); err != nil {
						return ActionResult{}, err
					}
				case "env":
					envPath := srv.envPathFor(p)
					orig, _ := os.ReadFile(envPath)
					if err := writeFilePreserving(envPath, b, true, false); err != nil {
						return ActionResult{}, err
					}
					if err := srv.persistPipelineEnv(p, envPath); err != nil {
						srv.rollbackFile(envPath, orig)
						return ActionResult{}, &ActionError{Status: http.StatusServiceUnavailable, Msg: ".env NOT adopted: metadata database: " + err.Error()}
					}
				default:
					return ActionResult{}, BadRequest("unknown conflict kind %q", c.File)
				}
				msg = "Adopted the disk version from " + c.Path + " (now in the database and on disk). Restart the pipeline to apply it."
			}
			all = append(all[:idx], all[idx+1:]...)
			nb, _ := json.Marshal(all)
			if len(all) == 0 {
				nb = []byte("[]")
			}
			if err := srv.db.st.SetPipelineConflicts(dctx, p.Name, string(nb)); err != nil {
				return ActionResult{}, &ActionError{Status: http.StatusServiceUnavailable, Msg: "metadata database: " + err.Error()}
			}
			srv.reg.Refresh(ctx)
			return ActionResult{Message: msg}, nil
		}
	}
	s.actions.Register(Action{Name: "conflict-adopt", Run: conflictOp(true)})
	s.actions.Register(Action{Name: "conflict-dismiss", Run: conflictOp(false)})
}

var _ = store.OriginScan
