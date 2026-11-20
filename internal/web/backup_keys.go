package web

import (
	"context"
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

type unencryptedUsage struct {
	Backups int   `json:"backups"`
	Bytes   int64 `json:"bytes"`
}

// keysInfo is the response of GET /api/v1/backup-keys (and the data of the Keys page).
type keysInfo struct {
	Configured  bool             `json:"configured"`
	Error       string           `json:"error,omitempty"`
	ActiveKeyID string           `json:"active_key_id,omitempty"`
	Keys        []keyView        `json:"keys"`
	Warnings    []keyWarning     `json:"warnings"`
	Unencrypted unencryptedUsage `json:"unencrypted"`
	Notice      string           `json:"notice,omitempty"`
}

func (s *Server) keysInfo(ctx context.Context) (keysInfo, error) {
	info := keysInfo{Keys: []keyView{}, Warnings: []keyWarning{}}
	usage, err := s.db.st.BackupKeyUsage(ctx)
	if err != nil {
		return info, err
	}
	byID := map[string]int{}
	for i, u := range usage {
		byID[u.KeyID] = i
	}
	kr, kerr := backup.LoadKeyring(os.Getenv)
	info.Configured = kr != nil
	if kerr != nil {
		info.Error = kerr.Error() // never contains key material (LoadKeyring guarantees it)
	}
	known := map[string]bool{}
	if kr != nil {
		for _, id := range kr.IDs() {
			k, _ := kr.Get(id)
			v := keyView{ID: id, Fingerprint: k.Fingerprint(), Active: kr.Active != nil && kr.Active.ID == id}
			if i, ok := byID[id]; ok {
				u := usage[i]
				v.Backups, v.Bytes, v.SampleIDs = u.Count, u.Bytes, u.SampleIDs
			}
			info.Keys = append(info.Keys, v)
			known[id] = true
		}
		info.ActiveKeyID = kr.Active.ID
	}
	sort.SliceStable(info.Keys, func(i, j int) bool { return info.Keys[i].Active && !info.Keys[j].Active })
	for _, u := range usage {
		if u.KeyID == "" {
			info.Unencrypted = unencryptedUsage{Backups: u.Count, Bytes: u.Bytes}
			continue
		}
		if !known[u.KeyID] {
			info.Warnings = append(info.Warnings, keyWarning{KeyID: u.KeyID, Backups: u.Count, SampleIDs: u.SampleIDs,
				Message: "these backups were written with a key that is no longer configured; they cannot be restored until it is added back to STRAND_BACKUP_ENCRYPTION_KEYS"})
		}
	}
	if kr == nil && kerr == nil {
		info.Notice = "no encryption key is configured (STRAND_BACKUP_ENCRYPTION_KEYS): new backups cannot be encrypted"
	}
	return info, nil
}

func (s *Server) apiBackupKeys(w http.ResponseWriter, r *http.Request, _ *Session) {
	if !s.needDB(w) {
		return
	}
	info, err := s.keysInfo(r.Context())
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, "could not read the backup catalog")
		return
	}
	writeJSON(w, http.StatusOK, info)
}
