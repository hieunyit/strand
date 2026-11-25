package web

// Supported language codes.
const (
	LangEN = "en"
	LangVI = "vi"
)

// SupportedLanguages returns the list of available language codes.
func SupportedLanguages() []string {
	return []string{LangEN, LangVI}
}

// messagesEn holds all English UI strings. Every key in messagesVi MUST exist here.
var messagesEn = map[string]string{
	// Common actions / buttons
	"btn.save":            "Save",
	"btn.save_changes":    "Save changes",
	"btn.cancel":          "Cancel",
	"btn.delete":          "Delete",
	"btn.edit":            "Edit",
	"btn.new":             "New",
	"btn.new_user":        "New user",
	"btn.new_pipeline":    "New pipeline",
	"btn.new_job":         "New backup job",
	"btn.new_connection":  "New connection",
	"btn.new_policy":      "New policy",
	"btn.new_repo":        "New repository",
	"btn.run_now":         "Run now",
	"btn.test_connection": "Test connection",
	"btn.start_restore":   "Start restore",
	"btn.create_user":     "Create user",
	"btn.close":           "Close",
	"btn.refresh":         "Refresh",
	"btn.view_log":        "View log",
	"btn.reset_password":  "Reset password",
	"btn.enable":          "Enable",
	"btn.disable":         "Disable",
	"btn.pause":           "Pause",
	"btn.resume":          "Resume",
	"btn.verify":          "Verify",

	// Sidebar navigation
	"nav.skip":                 "Skip to content",
	"nav.monitor":              "Monitor",
	"nav.data_movement":        "Data movement",
	"nav.data_protection":      "Data protection",
	"nav.system":               "System",
	"nav.overview":             "Overview",
	"nav.jobs":                 "Jobs",
	"nav.pipelines":            "Pipelines",
	"nav.new_pipeline":         "New pipeline",
	"nav.binlog_archive":       "Binlog archive",
	"nav.ssh_tunnels":          "SSH tunnels",
	"nav.spool_inspect":        "Spool inspect",
	"nav.tools":                "Tools",
	"nav.backup_resources":     "Backup resources",
	"nav.section_pages":        "Pages of this section",
	"nav.backups":              "Backup dashboard",
	"nav.backup_jobs":          "Backup jobs",
	"nav.restore_points":       "Restore points",
	"nav.restore":              "Restore",
	"nav.connections":          "Connections",
	"nav.policies":             "Policies",
	"nav.repositories":         "Repositories",
	"nav.reports":              "Reports",
	"nav.notifications":        "Notifications",
	"nav.encryption_keys":      "Encryption keys",
	"nav.backup_tools":         "Backup tools",
	"nav.activity":             "Activity",
	"nav.users":                "Users",
	"nav.settings":             "Settings",
	"nav.settings_general":     "General",
	"nav.settings_security":    "Security",
	"nav.settings_audit":       "Audit",
	"nav.settings_runtime":     "Runtime",
	"nav.metadata_database":    "Metadata database",
	"nav.mcp":                  "MCP",
	"nav.config_reference":     "Config reference",
	"nav.my_account":           "My account",
	"nav.registered_pipelines": "Registered pipelines",
	"nav.none_registered":      "none registered",

	// Top bar
	"top.search":       "Search",
	"top.search_hint":  "Search pipelines, backup jobs, pages and backup IDs",
	"top.connecting":   "Connecting",
	"top.connected":    "Connected",
	"top.paused":       "Paused",
	"top.signed_in_as": "Signed in as",
	"top.user_menu":    "User menu",
	"top.log_out":      "Log out",

	// Confirm dialog
	"confirm.title":           "Confirm",
	"confirm.type":            "Type",
	"confirm.to_confirm":      "to confirm",
	"confirm.type_to_confirm": "Confirmation text",
	"confirm.discard_changes": "Discard the changes you made in this form?",

	// Empty and error states
	"empty.no_pipelines":      "No pipelines registered",
	"empty.no_jobs":           "No jobs found",
	"empty.no_backups":        "No backup jobs found",
	"empty.no_restore_points": "No restore points found",
	"empty.no_users":          "No users found",
	"empty.all_healthy":       "All systems healthy",
	"empty.needs_attention":   "Needs attention",

	// Login page
	"login.title":                "Sign in",
	"login.tagline":              "Sign in to manage your replication pipelines.",
	"login.username":             "Username",
	"login.password":             "Password",
	"login.sign_in":              "Sign in",
	"login.expired":              "Your session expired. Sign in again.",
	"login.pitch_title":          "Change data capture, accounted for.",
	"login.pitch_sub":            "Replicate MySQL, MariaDB, PostgreSQL and MongoDB with checkpoints you can see, and run every pipeline from one console.",
	"login.bullet1":              "Snapshot and stream with live lag and progress",
	"login.bullet2":              "Verify, repair, promote and seed wizards",
	"login.bullet3":              "Role-based access and an audit log of every change",
	"login.first_login_prompt":   "First login: please set a new password. Must be at least 12 characters and not equal to your username.",
	"login.new_password":         "New password",
	"login.confirm_new_password": "Confirm new password",
	"login.set_password_button":  "Set new password & sign in",
	"login.forgot_hint":          "Forgot your password? Ask an administrator to reset it.",

	// Overview page
	"overview.title":               "Overview",
	"overview.subtitle":            "Replication pipelines, data protection status and system health",
	"overview.data_protection":     "Data protection",
	"overview.protected_sources":   "Protected sources",
	"overview.failing_jobs":        "Failing jobs",
	"overview.rpo_breaches":        "RPO breaches",
	"overview.last_test_restore":   "Last test restore",
	"overview.needs_attention":     "Needs attention",
	"overview.all_systems_healthy": "All systems healthy",
	"overview.synced_pipelines":    "Pipelines in sync",

	// Pipelines list page
	"pipelines.title":    "Pipelines",
	"pipelines.subtitle": "Active change data capture pipelines, streaming status and latency",

	// Pipeline tab help
	"pipeline.tabs_help":                 "Guide to pipeline operations",
	"pipeline.tab_help.checkpoint":       "See the source log position saved for CDC to resume from. Administrators can rewind a running MySQL-to-MySQL pipeline to replay changes.",
	"pipeline.tab_help.jobs":             "Run Verify to compare source and target data without repairing it. See live output and past jobs.",
	"pipeline.tab_help.checkpoint_mongo": "See the change stream resume token saved for CDC to resume from, per database.",
	"pipeline.tab_help.jobs_mongo":       "Run Verify to compare source and target collections (counts, hashes, documents by _id) without repairing them. See live output and past jobs.",
	"pipeline.tab_help.schema":           "Preview table definitions and review pending schema changes. An administrator can approve them for the target.",
	"pipeline.tab_help.objects":          "List MySQL views, routines, triggers, scheduled events, users and grants. Administrators can apply them in phases to a MySQL target.",
	"pipeline.tab_help.repair":           "Fix data differences in a MySQL-to-MySQL pipeline. The wizard guides an administrator through stopping CDC, repairing the target and restarting.",
	"pipeline.tab_help.promote":          "Switch application writes to the target. Use the dry run to check readiness. Emergency failover can lose changes the target has yet to apply.",
	"pipeline.tab_help.seed":             "Initialize the target from a backup or a remote clone, then start CDC from that position. Applying a seed replaces target data.",

	// Backups overview page
	"backups.title":    "Backups",
	"backups.subtitle": "Centralized backup management, policies and recovery points",

	// Jobs list page
	"jobs.title":    "Jobs",
	"jobs.subtitle": "Background tasks, verification runs and backup executions",

	// Breadcrumbs common labels
	"Home":              "Home",
	"Overview":          "Overview",
	"Pipelines":         "Pipelines",
	"Jobs":              "Jobs",
	"Backups":           "Backups",
	"Backup jobs":       "Backup jobs",
	"Restore points":    "Restore points",
	"Restore":           "Restore",
	"Connections":       "Connections",
	"Policies":          "Policies",
	"Repositories":      "Repositories",
	"Reports":           "Reports",
	"Notifications":     "Notifications",
	"Encryption keys":   "Encryption keys",
	"Activity":          "Activity",
	"Users":             "Users",
	"Settings":          "Settings",
	"Metadata database": "Metadata database",
	"My account":        "My account",
	"New pipeline":      "New pipeline",

	// Restore points by status (catalog list, detail, restore wizard)
	"rp.action.view_error":      "View error",
	"rp.action.delete":          "Delete",
	"rp.in_progress":            "In progress",
	"rp.status.queued":          "Queued",
	"rp.status.verifying":       "Verifying",
	"rp.status.expired":         "Expired",
	"rp.delete_confirm":         "Delete restore point %s and the partial objects it uploaded to its repository? This cannot be undone.",
	"rp.delete_expired_confirm": "Remove expired restore point %s from the catalog? Retention already deleted its objects.",
	"rp.note.failed":            "This backup failed: its payload is partial or missing, so it cannot be restored, verified or copied. Check the error below, then delete it.",
	"rp.note.expired":           "Retention deleted the objects of this backup; only its catalog record remains. It cannot be restored.",
	"rp.note.in_progress":       "This backup is still running. Restore, verification and copies become available when it completes.",
	"rp.error_title":            "Why this backup failed",
	"rp.error_run":              "Run",
	"rp.error_none":             "No error message was recorded for this restore point. Check the runs of its job and the job log.",
	"rp.error_run_link":         "Open the job runs",
	"rp.error_jobs_link":        "Open the process logs",
	"rp.not_restorable":         "This restore point cannot be restored.",
	"rp.not_restorable_hint":    "Only complete restore points can be restored. Choose one below or in the catalog.",
	"rp.complete_list_link":     "Complete restore points",
	"rp.wizard_not_complete":    "Not a complete restore point:",
}

// messagesVi holds all Vietnamese UI strings. Must be natural Vietnamese for operators.
var messagesVi = map[string]string{
	// Common actions / buttons
	"btn.save":            "Lưu",
	"btn.save_changes":    "Lưu thay đổi",
	"btn.cancel":          "Hủy",
	"btn.delete":          "Xóa",
	"btn.edit":            "Chỉnh sửa",
	"btn.new":             "Thêm mới",
	"btn.new_user":        "Thêm người dùng",
	"btn.new_pipeline":    "Tạo pipeline",
	"btn.new_job":         "Tạo job sao lưu",
	"btn.new_connection":  "Thêm kết nối",
	"btn.new_policy":      "Thêm chính sách",
	"btn.new_repo":        "Thêm kho lưu trữ",
	"btn.run_now":         "Chạy ngay",
	"btn.test_connection": "Kiểm tra kết nối",
	"btn.start_restore":   "Bắt đầu khôi phục",
	"btn.create_user":     "Tạo người dùng",
	"btn.close":           "Đóng",
	"btn.refresh":         "Làm mới",
	"btn.view_log":        "Xem log",
	"btn.reset_password":  "Đặt lại mật khẩu",
	"btn.enable":          "Kích hoạt",
	"btn.disable":         "Vô hiệu hóa",
	"btn.pause":           "Tạm dừng",
	"btn.resume":          "Tiếp tục",
	"btn.verify":          "Kiểm chứng",

	// Sidebar navigation
	"nav.skip":                 "Chuyển đến nội dung chính",
	"nav.monitor":              "Giám sát",
	"nav.data_movement":        "Luân chuyển dữ liệu",
	"nav.data_protection":      "Bảo vệ dữ liệu",
	"nav.system":               "Hệ thống",
	"nav.overview":             "Tổng quan",
	"nav.jobs":                 "Tác vụ",
	"nav.pipelines":            "Pipeline",
	"nav.new_pipeline":         "Tạo pipeline",
	"nav.binlog_archive":       "Lưu trữ binlog",
	"nav.ssh_tunnels":          "Đường hầm SSH",
	"nav.spool_inspect":        "Soi spool",
	"nav.tools":                "Công cụ",
	"nav.backup_resources":     "Tài nguyên sao lưu",
	"nav.section_pages":        "Các trang trong mục này",
	"nav.backups":              "Tổng quan sao lưu",
	"nav.backup_jobs":          "Job sao lưu",
	"nav.restore_points":       "Điểm khôi phục",
	"nav.restore":              "Khôi phục",
	"nav.connections":          "Kết nối",
	"nav.policies":             "Chính sách",
	"nav.repositories":         "Kho lưu trữ",
	"nav.reports":              "Báo cáo",
	"nav.notifications":        "Kênh thông báo",
	"nav.encryption_keys":      "Khóa mã hóa",
	"nav.backup_tools":         "Công cụ sao lưu",
	"nav.activity":             "Nhật ký hoạt động",
	"nav.users":                "Người dùng",
	"nav.settings":             "Cài đặt",
	"nav.settings_general":     "Chung",
	"nav.settings_security":    "Bảo mật",
	"nav.settings_audit":       "Nhật ký kiểm toán",
	"nav.settings_runtime":     "Môi trường chạy",
	"nav.metadata_database":    "Cơ sở dữ liệu metadata",
	"nav.mcp":                  "MCP",
	"nav.config_reference":     "Tài liệu cấu hình",
	"nav.my_account":           "Tài khoản của tôi",
	"nav.registered_pipelines": "Pipeline đã đăng ký",
	"nav.none_registered":      "Chưa có pipeline nào",

	// Top bar
	"top.search":       "Tìm kiếm",
	"top.search_hint":  "Tìm pipeline, job sao lưu, trang và mã backup",
	"top.connecting":   "Đang kết nối",
	"top.connected":    "Đã kết nối",
	"top.paused":       "Đã tạm dừng",
	"top.signed_in_as": "Đăng nhập với vai trò",
	"top.user_menu":    "Menu người dùng",
	"top.log_out":      "Đăng xuất",

	// Confirm dialog
	"confirm.title":           "Xác nhận",
	"confirm.type":            "Gõ",
	"confirm.to_confirm":      "để xác nhận",
	"confirm.type_to_confirm": "Nội dung xác nhận",
	"confirm.discard_changes": "Hủy bỏ các thay đổi bạn vừa nhập vào biểu mẫu?",

	// Empty and error states
	"empty.no_pipelines":      "Chưa có pipeline nào được đăng ký",
	"empty.no_jobs":           "Không có tác vụ nào",
	"empty.no_backups":        "Không có job sao lưu nào",
	"empty.no_restore_points": "Không có điểm khôi phục nào",
	"empty.no_users":          "Không có người dùng nào",
	"empty.all_healthy":       "Tất cả hệ thống hoạt động bình thường",
	"empty.needs_attention":   "Cần chú ý",

	// Login page
	"login.title":                "Đăng nhập",
	"login.tagline":              "Đăng nhập để quản trị luân chuyển dữ liệu và sao lưu.",
	"login.username":             "Tên đăng nhập",
	"login.password":             "Mật khẩu",
	"login.sign_in":              "Đăng nhập",
	"login.expired":              "Phiên làm việc đã hết hạn. Vui lòng đăng nhập lại.",
	"login.pitch_title":          "Change data capture, accounted for.",
	"login.pitch_sub":            "Đồng bộ dữ liệu MySQL, MariaDB, PostgreSQL và MongoDB với điểm kiểm soát trực quan trên một giao diện thống nhất.",
	"login.bullet1":              "Snapshot và streaming thời gian thực với độ trễ trực tiếp",
	"login.bullet2":              "Trợ lý kiểm chứng dữ liệu, sửa chữa, thăng cấp và khởi tạo",
	"login.bullet3":              "Phân quyền theo vai trò và nhật ký kiểm toán cho mọi thao tác",
	"login.first_login_prompt":   "Lần đầu đăng nhập: vui lòng đặt mật khẩu mới (tối thiểu 12 ký tự và khác tên đăng nhập).",
	"login.new_password":         "Mật khẩu mới",
	"login.confirm_new_password": "Xác nhận mật khẩu mới",
	"login.set_password_button":  "Lưu mật khẩu & Đăng nhập",
	"login.forgot_hint":          "Quên mật khẩu? Vui lòng liên hệ quản trị viên để đặt lại.",

	// Overview page
	"overview.title":               "Tổng quan",
	"overview.subtitle":            "Tình trạng pipeline luân chuyển dữ liệu, bảo vệ dữ liệu và sức khỏe hệ thống",
	"overview.data_protection":     "Bảo vệ dữ liệu",
	"overview.protected_sources":   "Nguồn được bảo vệ",
	"overview.failing_jobs":        "Job gặp lỗi",
	"overview.rpo_breaches":        "Vi phạm RPO",
	"overview.last_test_restore":   "Lần diễn tập khôi phục gần nhất",
	"overview.needs_attention":     "Cần chú ý",
	"overview.all_systems_healthy": "Tất cả hệ thống hoạt động bình thường",
	"overview.synced_pipelines":    "Pipeline đồng bộ",

	// Pipelines list page
	"pipelines.title":    "Pipeline",
	"pipelines.subtitle": "Danh sách pipeline đồng bộ dữ liệu, trạng thái truyền tải và độ trễ",

	// Pipeline tab help
	"pipeline.tabs_help":                 "Hướng dẫn các tab vận hành pipeline",
	"pipeline.tab_help.checkpoint":       "Xem vị trí log nguồn đã lưu, nơi CDC sẽ đọc tiếp. Quản trị viên có thể lùi checkpoint của pipeline MySQL → MySQL đang chạy để phát lại thay đổi.",
	"pipeline.tab_help.jobs":             "Chạy Verify để đối chiếu dữ liệu nguồn và đích mà không sửa dữ liệu. Xem kết quả trực tiếp và lịch sử các job.",
	"pipeline.tab_help.checkpoint_mongo": "Xem resume token của change stream đã lưu (theo từng database), nơi CDC sẽ đọc tiếp.",
	"pipeline.tab_help.jobs_mongo":       "Chạy Verify để đối chiếu collection nguồn và đích (số document, hash, từng document theo _id) mà không sửa dữ liệu. Xem kết quả trực tiếp và lịch sử các job.",
	"pipeline.tab_help.schema":           "Xem trước cấu trúc bảng và duyệt thay đổi schema đang chờ. Quản trị viên có thể phê duyệt để áp dụng lên đích.",
	"pipeline.tab_help.objects":          "Liệt kê view, routine, trigger, sự kiện định kỳ, tài khoản và quyền của MySQL. Quản trị viên có thể áp dụng theo từng giai đoạn lên đích MySQL.",
	"pipeline.tab_help.repair":           "Sửa dữ liệu lệch trong pipeline MySQL → MySQL. Trình hướng dẫn giúp quản trị viên dừng CDC, sửa dữ liệu ở đích rồi chạy lại.",
	"pipeline.tab_help.promote":          "Chuyển ứng dụng sang ghi vào đích. Chạy thử để kiểm tra điều kiện. Failover khẩn cấp có thể mất các thay đổi đích chưa áp dụng.",
	"pipeline.tab_help.seed":             "Khởi tạo dữ liệu đích từ bản sao lưu hoặc bản clone qua mạng, rồi chạy CDC từ vị trí tương ứng. Áp dụng seed sẽ thay thế dữ liệu ở đích.",

	// Backups overview page
	"backups.title":    "Sao lưu",
	"backups.subtitle": "Quản lý sao lưu tập trung, chính sách lưu trữ và điểm khôi phục",

	// Jobs list page
	"jobs.title":    "Tác vụ",
	"jobs.subtitle": "Các tác vụ nền, tiến trình kiểm chứng và sao lưu",

	// Breadcrumbs common labels
	"Home":              "Trang chủ",
	"Overview":          "Tổng quan",
	"Pipelines":         "Pipeline",
	"Jobs":              "Tác vụ",
	"Backups":           "Sao lưu",
	"Backup jobs":       "Job sao lưu",
	"Restore points":    "Điểm khôi phục",
	"Restore":           "Khôi phục",
	"Connections":       "Kết nối",
	"Policies":          "Chính sách",
	"Repositories":      "Kho lưu trữ",
	"Reports":           "Báo cáo",
	"Notifications":     "Kênh thông báo",
	"Encryption keys":   "Khóa mã hóa",
	"Activity":          "Nhật ký hoạt động",
	"Users":             "Người dùng",
	"Settings":          "Cài đặt",
	"Metadata database": "Cơ sở dữ liệu metadata",
	"My account":        "Tài khoản của tôi",
	"New pipeline":      "Tạo pipeline",

	// Restore points by status (catalog list, detail, restore wizard)
	"rp.action.view_error":      "Xem lỗi",
	"rp.action.delete":          "Xóa",
	"rp.in_progress":            "Đang thực hiện",
	"rp.status.queued":          "Đang chờ",
	"rp.status.verifying":       "Đang kiểm tra",
	"rp.status.expired":         "Đã hết hạn",
	"rp.delete_confirm":         "Xóa điểm khôi phục %s và các đối tượng dở dang mà nó đã tải lên kho lưu trữ? Không thể hoàn tác.",
	"rp.delete_expired_confirm": "Gỡ điểm khôi phục đã hết hạn %s khỏi danh mục? Chính sách lưu giữ đã xóa các đối tượng của nó.",
	"rp.note.failed":            "Bản sao lưu này thất bại: dữ liệu bị thiếu hoặc dở dang nên không thể khôi phục, kiểm chứng hay sao chép. Xem lỗi bên dưới rồi xóa nó.",
	"rp.note.expired":           "Chính sách lưu giữ đã xóa các đối tượng của bản sao lưu này; chỉ còn bản ghi trong danh mục. Không thể khôi phục.",
	"rp.note.in_progress":       "Bản sao lưu này vẫn đang chạy. Khôi phục, kiểm chứng và sao chép sẽ khả dụng khi nó hoàn tất.",
	"rp.error_title":            "Lý do sao lưu thất bại",
	"rp.error_run":              "Lần chạy",
	"rp.error_none":             "Không có thông báo lỗi nào được ghi cho điểm khôi phục này. Xem các lần chạy của job và log của job.",
	"rp.error_run_link":         "Mở các lần chạy của job",
	"rp.error_jobs_link":        "Mở log tiến trình",
	"rp.not_restorable":         "Không thể khôi phục từ điểm khôi phục này.",
	"rp.not_restorable_hint":    "Chỉ có thể khôi phục từ điểm khôi phục hoàn tất. Hãy chọn một điểm bên dưới hoặc trong danh mục.",
	"rp.complete_list_link":     "Các điểm khôi phục hoàn tất",
	"rp.wizard_not_complete":    "Không phải điểm khôi phục hoàn tất:",
}

// T translates a key into the target language.
// If target language is "vi", it looks up messagesVi. If missing, it falls back to messagesEn.
// If target language is "en" or any other, it looks up messagesEn.
// If key is not in the dictionary, key itself is returned.
func T(lang, key string) string {
	if lang == LangVI {
		if val, ok := messagesVi[key]; ok && val != "" {
			return val
		}
	}
	if val, ok := messagesEn[key]; ok && val != "" {
		return val
	}
	return key
}
