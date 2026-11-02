package config

// JSONSchema trả về JSON Schema (Draft-07) của file cấu hình Strand theo spec/02-specs.md §10
const JSONSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "StrandConfig",
  "description": "Cấu hình pipeline Strand CDC theo spec/02-specs.md §10",
  "type": "object",
  "required": ["version", "name", "source", "target", "tables"],
  "additionalProperties": false,
  "properties": {
    "version": {
      "type": "integer",
      "description": "Phiên bản cấu hình (mặc định: 1)"
    },
    "name": {
      "type": "string",
      "description": "Tên pipeline (cũng là pipeline_id)"
    },
    "source": {
      "type": "object",
      "required": ["kind", "dsn"],
      "additionalProperties": false,
      "properties": {
        "kind": { "type": "string", "enum": ["mysql", "postgres"] },
        "dsn": { "type": "string" },
        "server_id": { "type": "integer" },
        "read_from": { "type": "string", "enum": ["replica", "primary"] }
      }
    },
    "target": {
      "type": "object",
      "required": ["kind", "dsn"],
      "additionalProperties": false,
      "properties": {
        "kind": { "type": "string", "enum": ["mysql", "postgres"] },
        "dsn": { "type": "string" },
        "write_mode": { "type": "string", "enum": ["upsert", "insert"] },
        "commit_order": { "type": "string", "enum": ["strict", "relaxed"] },
        "batch": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "max_rows": { "type": "integer" },
            "max_bytes": { "type": "string" },
            "max_wait": { "type": "string" }
          }
        }
      }
    },
    "tables": {
      "type": "object",
      "required": ["include"],
      "additionalProperties": false,
      "properties": {
        "include": { "type": "array", "items": { "type": "string" } },
        "exclude": { "type": "array", "items": { "type": "string" } },
        "rename": {
          "type": "object",
          "additionalProperties": { "type": "string" }
        }
      }
    },
    "bootstrap": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "default": { "type": "string", "enum": ["logical", "seed", "start_at"] },
        "per_table": {
          "type": "object",
          "additionalProperties": {
            "type": "object",
            "properties": {
              "mode": { "type": "string", "enum": ["seed", "logical"] },
              "seed": {
                "type": "object",
                "properties": {
                  "type": { "type": "string", "enum": ["xtrabackup", "mydumper", "clone", "pgbackrest"] },
                  "uri": { "type": "string" }
                }
              }
            }
          }
        },
        "catch_up": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "binlog_archive": { "type": "string" }
          }
        },
        "logical": {
          "type": "object",
          "additionalProperties": false,
          "properties": {
            "chunk_rows": { "type": "string" },
            "max_parallel": { "type": "integer" },
            "windows": { "type": "array", "items": { "type": "string" } }
          }
        }
      }
    },
    "schema": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "additive": { "type": "string", "enum": ["auto", "pause"] },
        "destructive": { "type": "string", "enum": ["pause", "approve"] }
      }
    },
    "checkpoint": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "store": { "type": "string" },
        "flush_every": { "type": "string" }
      }
    },
    "resources": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "memory": {
          "type": "object",
          "properties": {
            "soft": { "type": "string" },
            "hard": { "type": "string" }
          }
        },
        "spool": {
          "type": "object",
          "properties": {
            "path": { "type": "string" },
            "soft": { "type": "string" },
            "hard": { "type": "string" },
            "overflow": { "type": "string" }
          }
        },
        "source": {
          "type": "object",
          "properties": {
            "iops": {
              "type": "object",
              "properties": {
                "soft": { "type": "integer" },
                "hard": { "type": "integer" }
              }
            }
          }
        },
        "target": {
          "type": "object",
          "properties": {
            "iops": {
              "type": "object",
              "properties": {
                "soft": { "type": "integer" },
                "hard": { "type": "integer" }
              }
            }
          }
        },
        "workers": {
          "type": "object",
          "properties": {
            "min": { "type": "integer" },
            "max": { "type": "integer" }
          }
        }
      }
    },
    "governor": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "algorithm": { "type": "string", "enum": ["grace-v2", "onoff", "static"] },
        "tick": { "type": "string" },
        "lag_slo": { "type": "string" },
        "probe": { "type": "string", "enum": ["on", "off"] },
        "breaker": { "type": "object" },
        "metrics_source": { "type": "object" }
      }
    },
    "verify": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "delta": { "type": "string", "enum": ["on", "off"] },
        "post_seed_window": { "type": "string" },
        "full": {
          "type": "object",
          "properties": {
            "cron": { "type": "string" },
            "rate": { "type": "string" }
          }
        }
      }
    },
    "observability": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "metrics": { "type": "string" },
        "admin_socket": { "type": "string" },
        "log": {
          "type": "object",
          "properties": {
            "level": { "type": "string", "enum": ["debug", "info", "warn", "error"] },
            "format": { "type": "string", "enum": ["json", "text"] }
          }
        }
      }
    }
  }
}`
