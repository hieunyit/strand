package backup

import (
	"strings"
	"testing"
)

func TestUnsupportedBackupMethods(t *testing.T) {
	if !IsUnsupportedMethod("xtrabackup") {
		t.Fatal("expected xtrabackup to be unsupported")
	}
	if !IsUnsupportedMethod("mariabackup") {
		t.Fatal("expected mariabackup to be unsupported")
	}
	if IsUnsupportedMethod("mysqldump") {
		t.Fatal("mysqldump should be supported")
	}

	baseJob := Job{
		ID:           "bjob-test",
		Name:         "test job",
		EverySeconds: 3600,
		Bucket:       "test-bucket",
		JobOptions: JobOptions{
			Source: "mysql-prod",
			Conn: ConnSpec{
				Host:        "127.0.0.1",
				Port:        3306,
				User:        "root",
				PasswordEnv: "PW",
				Databases:   []string{"db"},
			},
		},
	}

	for _, method := range []string{"xtrabackup", "mariabackup"} {
		j := baseJob
		j.Method = method
		err := ValidateJob(j)
		if err == nil || !strings.Contains(err.Error(), "unsupported") || !strings.Contains(err.Error(), "removed") {
			t.Fatalf("expected unsupported (removed) error for %s, got: %v", method, err)
		}
	}

	// Incremental chain creation (FullEvery > 1) is no longer supported
	j := baseJob
	j.Method = MethodMySQLDump
	j.FullEvery = 7
	err := ValidateJob(j)
	if err == nil || !strings.Contains(err.Error(), "incremental backup chains") {
		t.Fatalf("expected incremental chains not supported error, got: %v", err)
	}
}

func TestValidateJob_PITRConstraints(t *testing.T) {
	baseJob := Job{
		ID:           "bjob-pitr",
		Name:         "pitr test job",
		EverySeconds: 3600,
		Bucket:       "test-bucket",
		JobOptions: JobOptions{
			Source: "test-source",
			Conn: ConnSpec{
				Host:        "127.0.0.1",
				Port:        3306,
				User:        "root",
				PasswordEnv: "PW",
				Databases:   []string{"db"},
			},
			LogArchive: true,
		},
	}

	// 1. MariaDB / mariadb-dump: rejected
	{
		j := baseJob
		j.Method = MethodMariaDBDump
		err := ValidateJob(j)
		if err == nil || !strings.Contains(err.Error(), "MariaDB does not support binlog archiving") {
			t.Fatalf("expected MariaDB binlog archiving error, got: %v", err)
		}
	}

	// 2. PostgreSQL: pg_dump rejected
	{
		j := baseJob
		j.Method = MethodPGDump
		j.Conn.Port = 5432
		err := ValidateJob(j)
		if err == nil || !strings.Contains(err.Error(), "PITR needs pg_basebackup as the base backup") {
			t.Fatalf("expected pg_dump PITR error, got: %v", err)
		}
	}

	// 3. PostgreSQL: pg_basebackup accepted
	{
		j := baseJob
		j.Method = MethodPGBaseBackup
		j.Conn.Port = 5432
		j.Conn.Databases = nil
		if err := ValidateJob(j); err != nil {
			t.Fatalf("pg_basebackup with log_archive should be valid: %v", err)
		}
	}

	// 4. MySQL: mysqldump with multiple databases rejected
	{
		j := baseJob
		j.Method = MethodMySQLDump
		j.Conn.Databases = []string{"db1", "db2"}
		err := ValidateJob(j)
		if err == nil || !strings.Contains(err.Error(), "PITR requires exactly one database") {
			t.Fatalf("expected multi-db PITR error, got: %v", err)
		}
	}

	// 5. MySQL: mysqldump with zero databases rejected
	{
		j := baseJob
		j.Method = MethodMySQLDump
		j.Conn.Databases = []string{}
		err := ValidateJob(j)
		if err == nil || !strings.Contains(err.Error(), "PITR requires exactly one database") {
			t.Fatalf("expected zero-db PITR error, got: %v", err)
		}
	}

	// 6. MySQL: mysqldump with exactly one database accepted
	{
		j := baseJob
		j.Method = MethodMySQLDump
		j.Conn.Databases = []string{"db1"}
		if err := ValidateJob(j); err != nil {
			t.Fatalf("mysqldump with single db and log_archive should be valid: %v", err)
		}
	}

	// 7. MongoDB: mongodump accepted
	{
		j := baseJob
		j.Method = MethodMongodump
		j.Conn.Host = "127.0.0.1"
		j.Conn.Port = 27017
		if err := ValidateJob(j); err != nil {
			t.Fatalf("mongodump with log_archive should be valid: %v", err)
		}
	}

	// 8. Custom: rejected
	{
		j := baseJob
		j.Method = MethodCustom
		j.Command = "/bin/backup"
		j.ArtifactTemplate = "/var/backups/{id}.dump"
		j.ManifestTemplate = "/var/backups/{id}.json"
		err := ValidateJob(j)
		if err == nil || !strings.Contains(err.Error(), "built-in method") {
			t.Fatalf("expected custom log archive error, got: %v", err)
		}
	}
}
