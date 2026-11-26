package web

import (
	"context"
	"errors"
	"path/filepath"
	"regexp"
	"strings"
)

// Thao tác CLI còn thiếu trên web, chạy thành job (output trực tiếp trên trang job, lịch sử trong DB):
//   - plan:                 strand plan -c <cfg>
//   - schema-diff:          strand schema diff -c <cfg> [--all]          (thay đổi schema chờ duyệt + DDL đề xuất)
//   - schema-approve-dry:   strand schema approve <id> -c <cfg> --dry-run (in DDL sẽ áp)
//   - schema-approve:       strand schema approve <id> -c <cfg>          (áp DDL lên target; gõ lại tên pipeline)
//   - schema-init-dry:      strand schema init -c <cfg> --dry-run        (DDL của bảng còn thiếu ở target)
//   - objects-list:         strand objects list -c <cfg>                 (view/routine/trigger/event/user; nguồn MySQL)
//   - objects-apply:        strand objects apply -c <cfg> --phase pre|post|users (target MySQL; gõ lại tên)
const (
	ActPlan             = "plan"
	ActSchemaDiff       = "schema-diff"
	ActSchemaApproveDry = "schema-approve-dry"
	ActSchemaApprove    = "schema-approve"
	ActSchemaInitDry    = "schema-init-dry"
	ActObjectsList      = "objects-list"
	ActObjectsApply     = "objects-apply"
	ActExport           = "export"
)

var schemaChangeIDRE = regexp.MustCompile(`^[0-9]{1,18}$`)

func (s *Server) registerCLIOps() {
	reg := func(name string, confirm bool, build func(p *Pipeline, args map[string]string) ([]string, error)) {
		s.actions.Register(Action{Name: name, ConfirmName: confirm, Run: func(ctx context.Context, srv *Server, req ActionRequest) (ActionResult, error) {
			argv, err := build(req.Pipeline, req.Args)
			if err != nil {
				return ActionResult{}, err
			}
			return srv.startCLIJob(ctx, req.Pipeline, name, argv)
		}})
	}
	reg(ActPlan, false, func(p *Pipeline, _ map[string]string) ([]string, error) {
		return []string{"plan", "-c", p.ConfigPath}, nil
	})
	reg(ActSchemaDiff, false, func(p *Pipeline, a map[string]string) ([]string, error) {
		argv := []string{"schema", "diff", "-c", p.ConfigPath}
		if truthyArg(a["all"]) {
			argv = append(argv, "--all")
		}
		return argv, nil
	})
	approve := func(dry bool) func(p *Pipeline, a map[string]string) ([]string, error) {
		return func(p *Pipeline, a map[string]string) ([]string, error) {
			id := strings.TrimSpace(a["id"])
			if !schemaChangeIDRE.MatchString(id) {
				return nil, BadRequest("id: the number of a pending schema change (see Show pending changes)")
			}
			argv := []string{"schema", "approve", id, "-c", p.ConfigPath}
			if dry {
				argv = append(argv, "--dry-run")
			}
			return argv, nil
		}
	}
	reg(ActSchemaApproveDry, false, approve(true))
	reg(ActSchemaApprove, true, approve(false))
	reg(ActSchemaInitDry, false, func(p *Pipeline, _ map[string]string) ([]string, error) {
		return []string{"schema", "init", "-c", p.ConfigPath, "--dry-run"}, nil
	})
	reg(ActObjectsList, false, func(p *Pipeline, _ map[string]string) ([]string, error) {
		if p.Cfg != nil && !strings.EqualFold(p.Cfg.Source.Kind, "mysql") {
			return nil, BadRequest("objects are transferred from a MySQL/MariaDB source only")
		}
		return []string{"objects", "list", "-c", p.ConfigPath}, nil
	})
	reg(ActObjectsApply, true, func(p *Pipeline, a map[string]string) ([]string, error) {
		if p.Cfg != nil && (!strings.EqualFold(p.Cfg.Source.Kind, "mysql") || !strings.EqualFold(p.Cfg.Target.Kind, "mysql")) {
			return nil, BadRequest("objects apply needs a MySQL/MariaDB source and a MySQL target")
		}
		switch ph := strings.TrimSpace(a["phase"]); ph {
		case "pre", "post", "users":
			return []string{"objects", "apply", "-c", p.ConfigPath, "--phase", ph}, nil
		default:
			return nil, BadRequest("phase must be pre, post or users")
		}
	})
	reg(ActExport, false, func(p *Pipeline, a map[string]string) ([]string, error) {
		out := strings.TrimSpace(a["out"])
		allowedDir := filepath.Clean(filepath.Join(s.cfg.StateDir, "exports"))
		if out == "" {
			out = filepath.Join(allowedDir, p.Name)
		} else {
			cleanOut := filepath.Clean(out)
			if cleanOut != allowedDir && !strings.HasPrefix(cleanOut, allowedDir+string(filepath.Separator)) {
				return nil, BadRequest("out: export directory must be within %s", allowedDir)
			}
			out = cleanOut
		}
		return []string{"export", "-c", p.ConfigPath, "--out", out}, nil
	})
}

func truthyArg(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "true", "1", "yes", "on":
		return true
	}
	return false
}

// startCLIJob chạy `strand <argv>` thành job của pipeline (một job mỗi pipeline), sau khi dựng lại file
// cấu hình/.env từ DB (tiến trình strand chỉ đọc file).
func (s *Server) startCLIJob(ctx context.Context, p *Pipeline, kind string, argv []string) (ActionResult, error) {
	if s.jobRunner() == nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: "job runner not configured"}
	}
	s.materialize(p.Name)
	env, err := p.Env()
	if err != nil {
		return ActionResult{}, &ActionError{Status: 500, Msg: "env_file: " + err.Error()}
	}
	id, err := s.jobRunner().Start(ctx, JobSpec{Pipeline: p.Name, Kind: kind, Args: argv, Env: env})
	if err != nil {
		var run *ErrJobRunning
		if errors.As(err, &run) {
			return ActionResult{}, Conflict("%v", err)
		}
		return ActionResult{}, &ActionError{Status: 502, Msg: err.Error()}
	}
	return ActionResult{Message: kind + " job started", JobID: id}, nil
}
