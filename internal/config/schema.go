package config

// JSONSchema trả về JSON Schema (Draft-07) của file cấu hình Strand theo spec/02-specs.md §10
const JSONSchema = `{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "StrandConfig",
  "description": "Cấu hình pipeline Strand CDC theo spec/02-specs.md §10",
  "type": "object",
  "required": ["version", "name", "source", "target", "tables"],
  "additionalProperties": false,
  "definitions": {
    "byteRate": {
      "type": "object",
      "description": "Cặp soft/hard theo kích thước mỗi giây, ví dụ { soft: 40MiB, hard: 60MiB } (chưa triển khai, validate cảnh báo)",
      "additionalProperties": false,
      "properties": {
        "soft": { "type": "string" },
        "hard": { "type": "string" }
      }
    }
  },
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
      "required": ["kind"],
      "additionalProperties": false,
      "properties": {
        "kind": { "type": "string", "enum": ["mysql", "postgres"] },
        "dsn": { "type": "string", "description": "mysql://user:pass@host:3306/db hoặc user:pass@tcp(host:3306)/db; bỏ trống khi dùng host/user" },
        "host": { "type": "string" },
        "port": { "type": "integer" },
        "user": { "type": "string" },
        "password": { "type": "string" },
        "password_env": { "type": "string", "description": "Tên biến môi trường chứa mật khẩu" },
        "database": { "type": "string" },
        "server_id": { "type": "integer" },
        "read_from": { "type": "string", "enum": ["primary", "replica", "standby"], "description": "standby: decode trên standby PG16+ (chưa triển khai, validate cảnh báo)" }
      }
    },
    "target": {
      "type": "object",
      "required": ["kind"],
      "additionalProperties": false,
      "properties": {
        "kind": { "type": "string", "enum": ["mysql", "postgres"] },
        "dsn": { "type": "string", "description": "mysql://user:pass@host:3306/strand hoặc user:pass@tcp(host:3306)/strand; bỏ trống khi dùng host/user" },
        "host": { "type": "string" },
        "port": { "type": "integer" },
        "user": { "type": "string" },
        "password": { "type": "string" },
        "password_env": { "type": "string", "description": "Tên biến môi trường chứa mật khẩu" },
        "database": { "type": "string" },
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
        },
        "mask": {
          "type": "object",
          "description": "Cột -> hàm mask, ví dụ \"app.users.phone\": sha256; chỉ cho snapshot/seed logic (chưa triển khai, validate cảnh báo)",
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
                  "type": { "type": "string", "enum": ["xtrabackup", "clone", "mydumper", "mysqldump", "pgbackrest", "walg", "strand-export", "volume-snapshot"] },
                  "uri": { "type": "string" },
                  "credentials": { "type": "string" },
                  "encryption_key": { "type": "string" },
                  "load": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                      "threads": { "type": "integer" },
                      "defer_indexes": { "type": "boolean" },
                      "bandwidth": { "type": "string" }
                    }
                  },
                  "verify_after": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                      "sample_ranges": { "type": "string" },
                      "delta_window": { "type": "string" }
                    }
                  }
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
            },
            "bandwidth": { "$ref": "#/definitions/byteRate" }
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
            },
            "bandwidth": { "$ref": "#/definitions/byteRate" }
          }
        },
        "workers": {
          "type": "object",
          "properties": {
            "min": { "type": "integer" },
            "max": { "type": "integer" }
          }
        },
        "network": {
          "type": "object",
          "description": "Giới hạn băng thông mạng (chưa triển khai, validate cảnh báo)",
          "additionalProperties": false,
          "properties": {
            "backup_read": { "$ref": "#/definitions/byteRate" }
          }
        },
        "schedule": {
          "type": "array",
          "description": "Đổi limit theo lịch; ngoài cron là các key dạng đường dẫn chấm, ví dụ source.iops.soft (chưa triển khai, validate cảnh báo)",
          "items": {
            "type": "object",
            "required": ["cron"],
            "properties": {
              "cron": { "type": "string" }
            },
            "additionalProperties": { "type": ["string", "number", "boolean"] }
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
