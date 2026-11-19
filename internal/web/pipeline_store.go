package web

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"strand/internal/config"
	"strand/internal/web/store"
)

// LoadStoredPipeline loads a pipeline configuration and its encrypted environment
// directly from the metadata database. It does not create or read pipeline files.
func LoadStoredPipeline(ctx context.Context, st store.Store, name string, pathsOnly bool) (*config.Config, []string, error) {
	if st == nil || !ValidName(name) {
		return nil, nil, errors.New("a metadata store and valid pipeline name are required")
	}
	row, ok, err := st.GetPipeline(ctx, name)
	if err != nil {
		return nil, nil, err
	}
	if !ok {
		return nil, nil, fmt.Errorf("pipeline %q was not found in the metadata database", name)
	}

	var raw []byte
	switch {
	case len(row.ConfigJSON) > 0:
		raw = row.ConfigJSON
	case row.HasConfig:
		// Read old Web DB rows during migration. New Web-created rows use ConfigJSON.
		raw = []byte(row.ConfigYAML)
	case row.ConfigPath != "":
		// Compatibility for externally managed legacy pipelines. New Web-created
		// pipelines have no path and never take this branch.
		cfg, err := config.LoadFile(row.ConfigPath)
		return cfg, nil, err
	default:
		return nil, nil, fmt.Errorf("pipeline %q has no stored configuration", name)
	}

	if pathsOnly {
		var cfg config.Config
		if err := yaml.Unmarshal(raw, &cfg); err != nil {
			return nil, nil, fmt.Errorf("decode stored pipeline configuration: %w", err)
		}
		return &cfg, nil, nil
	}

	var env []string
	if len(row.EnvEnc) > 0 {
		secret := strings.TrimSpace(os.Getenv(EnvSecretKey))
		if secret == "" {
			return nil, nil, fmt.Errorf("pipeline %q has encrypted connection secrets; set %s for the Strand process", name, EnvSecretKey)
		}
		box, err := store.NewBox(secret)
		if err != nil {
			return nil, nil, err
		}
		plain, err := box.Open(row.EnvEnc, name)
		if err != nil {
			return nil, nil, fmt.Errorf("decrypt pipeline environment: %w", err)
		}
		env, err = ParseEnvData(strings.NewReader(string(plain)))
		if err != nil {
			return nil, nil, err
		}
	}

	// Pipeline-specific encrypted values override process-wide variables, matching
	// the former EnvironmentFile behavior without mutating process-global state.
	envMap := make(map[string]string, len(env))
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		envMap[key] = value
	}
	cfg, err := config.LoadYAMLStrictWithEnv(raw, envMap)
	if err != nil {
		return nil, nil, err
	}
	if cfg.Name != name {
		return nil, nil, fmt.Errorf("stored pipeline name %q does not match registry name %q", cfg.Name, name)
	}
	return cfg, env, nil
}

// StorePipelineConfig saves the canonical, typed config and encrypted environment.
func StorePipelineConfig(ctx context.Context, st store.Store, name string, cfg *config.Config, env []byte) error {
	if st == nil || cfg == nil || cfg.Name != name || !ValidName(name) {
		return errors.New("invalid stored pipeline configuration")
	}
	encoded, err := json.Marshal(cfg)
	if err != nil {
		return err
	}
	row, ok, err := st.GetPipeline(ctx, name)
	if err != nil {
		return err
	}
	if !ok {
		row = store.Pipeline{Name: name, Origin: store.OriginCreated}
	}
	row.ConfigPath = ""
	row.EnvPath = ""
	row.ConfigYAML = ""
	row.HasConfig = false
	row.ConfigJSON = encoded
	row.EnvEnc = nil
	if len(env) > 0 {
		secret := strings.TrimSpace(os.Getenv(EnvSecretKey))
		if secret == "" {
			return fmt.Errorf("set %s to encrypt and save pipeline connection secrets", EnvSecretKey)
		}
		box, err := store.NewBox(secret)
		if err != nil {
			return err
		}
		row.EnvEnc, err = box.Seal(env, name)
		if err != nil {
			return err
		}
	}
	return st.UpsertPipeline(ctx, row)
}
