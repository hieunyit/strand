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
