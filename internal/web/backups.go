package web

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"strand/internal/backup"
)

type backupsPageBody struct {
	Backups    []backup.Record
	Message    string
	NextCursor string
	Paged      bool
}

const backupsPageSize = 200

func (s *Server) pageBackups(w http.ResponseWriter, r *http.Request, sess *Session) {
	b := backupsPageBody{}
	if s.backups == nil {
		b.Message = "Central backup catalog is unavailable: configure the web metadata database."
	} else {
		f := backup.Filter{Limit: backupsPageSize}
		if v := r.URL.Query().Get("cursor"); v != "" {
			c, err := backup.ParseCursor(v)
			if err != nil {
				http.Error(w, "invalid cursor", http.StatusBadRequest)
				return
			}
			f.After, b.Paged = &c, true
		}
		items, err := s.backups.List(r.Context(), f)
		if err != nil {
			s.log.Error("list backup catalog", "error", err)
			b.Message = "Could not read the backup catalog."
		} else {
			b.Backups = items
			b.NextCursor = backup.NextCursor(items, f.Limit)
		}
	}
	s.render(w, "backups", sess, pageData{Title: "Backups", Active: "backups", Body: b})
}

func (s *Server) apiBackups(w http.ResponseWriter, r *http.Request, _ *Session) {
	if s.backups == nil {
		writeErr(w, http.StatusServiceUnavailable, "central backup catalog requires the metadata database")
		return
	}
	f := backup.Filter{Source: r.URL.Query().Get("source"), Engine: r.URL.Query().Get("engine"), Status: backup.Status(r.URL.Query().Get("status")), Limit: 200}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > backup.MaxListLimit {
			writeErr(w, http.StatusBadRequest, "limit must be between 1 and 500")
			return
		}
		f.Limit = n
	}
	var err error
	if v := r.URL.Query().Get("created_after"); v != "" {
		t, parseErr := time.Parse(time.RFC3339Nano, v)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, "created_after must be RFC3339")
			return
		}
		f.CreatedAfter = &t
	}
	if v := r.URL.Query().Get("created_before"); v != "" {
		t, parseErr := time.Parse(time.RFC3339Nano, v)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, "created_before must be RFC3339")
			return
		}
		f.CreatedBefore = &t
	}
	if v := r.URL.Query().Get("cursor"); v != "" {
		c, parseErr := backup.ParseCursor(v)
		if parseErr != nil {
			writeErr(w, http.StatusBadRequest, "cursor is malformed")
			return
		}
		f.After = &c
	}
	items, err := s.backups.List(r.Context(), f)
	if err != nil {
		if errors.Is(err, backup.ErrInvalid) {
			writeErr(w, http.StatusBadRequest, err.Error())
		} else {
			s.log.Error("list backup catalog", "error", err)
			writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		}
		return
	}
	out := map[string]any{"backups": items, "limit": f.Limit}
	if next := backup.NextCursor(items, f.Limit); next != "" {
		out["next_cursor"] = next
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) apiBackup(w http.ResponseWriter, r *http.Request, _ *Session) {
	rc, found, ok := s.lookupBackup(w, r)
	if !ok {
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, rc)
}

func (s *Server) apiBackupStatus(w http.ResponseWriter, r *http.Request, _ *Session) {
	rc, found, ok := s.lookupBackup(w, r)
	if !ok {
		return
	}
	if !found {
		writeErr(w, http.StatusNotFound, "backup not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"id": rc.ID, "status": rc.Status, "created_at": rc.CreatedAt,
		"verified_at": rc.VerifiedAt, "expires_at": rc.ExpiresAt,
	})
}

func (s *Server) lookupBackup(w http.ResponseWriter, r *http.Request) (backup.Record, bool, bool) {
	if s.backups == nil {
		writeErr(w, http.StatusServiceUnavailable, "central backup catalog requires the metadata database")
		return backup.Record{}, false, false
	}
	rc, found, err := s.backups.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		if errors.Is(err, backup.ErrInvalid) {
			writeErr(w, http.StatusBadRequest, err.Error())
		} else {
			s.log.Error("get backup catalog record", "error", err)
			writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		}
		return backup.Record{}, false, false
	}
	return rc, found, true
}
