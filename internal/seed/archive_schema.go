package seed

import (
	archivereader "strand/internal/reader/archive"
)

// ArchiveIdentityMode định nghĩa identity mode cho bảng archive binlog.
type ArchiveIdentityMode = archivereader.IdentityMode

const (
	ArchiveIdentityModePK      = archivereader.IdentityModePK
	ArchiveIdentityModeUnique  = archivereader.IdentityModeUnique
	ArchiveIdentityModeKeyless = archivereader.IdentityModeKeyless
)

// ArchiveTableIdentity biểu diễn identity contract đã xác minh của một bảng trong archive binlog.
type ArchiveTableIdentity = archivereader.TableIdentity

// ArchiveSchemaSidecar biểu diễn sidecar schema bất biến từ snapshot hoặc operator.
type ArchiveSchemaSidecar = archivereader.SchemaSidecar

// LoadArchiveSchemaSidecar tải schema sidecar từ file JSON.
func LoadArchiveSchemaSidecar(path string) (*ArchiveSchemaSidecar, error) {
	return archivereader.LoadSchemaSidecar(path)
}
