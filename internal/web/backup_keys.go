package web

import (
	"net/http"
	"os"
	"sort"

	"strand/internal/backup"
)

// Encryption keys page data (admin only; GET /api/v1/backup-keys is in readRules). Key ids come from
// STRAND_BACKUP_ENCRYPTION_KEYS (or the file/single-key variants); the FIRST key is the active write key.
// Only ids and a short non-reversible fingerprint are ever returned, never key material, and a catalog backup
// whose key id is not configured any more is flagged because it can no longer be restored.

func (s *Server) backupKeyRoutes(mux *http.ServeMux) {
	mux.Handle("GET /api/v1/backup-keys", s.auth(true, s.apiBackupKeys))
}

type keyView struct {
	ID          string   `json:"id"`
	Fingerprint string   `json:"fingerprint"`
	Active      bool     `json:"active"` // the key new backups are written with
	Backups     int      `json:"backups"`
	Bytes       int64    `json:"bytes"`
	SampleIDs   []string `json:"sample_backup_ids,omitempty"`
}

type keyWarning struct {
	KeyID     string   `json:"key_id"`
	Backups   int      `json:"backups"`
	SampleIDs []string `json:"sample_backup_ids,omitempty"`
	Message   string   `json:"message"`
}

func (s *Server) apiBackupKeys(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	usage, err := s.db.st.BackupKeyUsage(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	byID := map[string]int{}
	for i, u := range usage {
		byID[u.KeyID] = i
	}
	kr, kerr := backup.LoadKeyring(os.Getenv)
	resp := map[string]any{"configured": kr != nil, "keys": []keyView{}, "warnings": []keyWarning{}}
	if kerr != nil {
		// The message never contains key material (LoadKeyring guarantees it).
		resp["error"] = kerr.Error()
	}
	var keys []keyView
	known := map[string]bool{}
	if kr != nil {
		for _, id := range kr.IDs() {
			k, _ := kr.Get(id)
			v := keyView{ID: id, Fingerprint: k.Fingerprint(), Active: kr.Active != nil && kr.Active.ID == id}
			if i, ok := byID[id]; ok {
				u := usage[i]
				v.Backups, v.Bytes, v.SampleIDs = u.Count, u.Bytes, u.SampleIDs
			}
			keys = append(keys, v)
			known[id] = true
		}
		resp["active_key_id"] = kr.Active.ID
	}
	sort.SliceStable(keys, func(i, j int) bool { return keys[i].Active && !keys[j].Active })
	if keys != nil {
		resp["keys"] = keys
	}
	var warns []keyWarning
	unenc := map[string]any{"backups": 0, "bytes": int64(0)}
	for _, u := range usage {
		if u.KeyID == "" {
			unenc = map[string]any{"backups": u.Count, "bytes": u.Bytes}
			continue
		}
		if !known[u.KeyID] {
			warns = append(warns, keyWarning{KeyID: u.KeyID, Backups: u.Count, SampleIDs: u.SampleIDs,
				Message: "these backups were written with a key that is no longer configured; they cannot be restored until it is added back to STRAND_BACKUP_ENCRYPTION_KEYS"})
		}
	}
	if warns != nil {
		resp["warnings"] = warns
	}
	resp["unencrypted"] = unenc
	if kr == nil && kerr == nil {
		resp["notice"] = "no encryption key is configured (STRAND_BACKUP_ENCRYPTION_KEYS): new backups cannot be encrypted"
	}
	writeJSON(w, http.StatusOK, resp)
}
