package runner

import (
	"io"
	"strings"
	"testing"
)

func TestMySQLPartialFilterGolden(t *testing.T) {
	// Golden input with 3 tables:
	// - t1: has view v_t1 and trigger tr_t1
	// - t2: has trigger tr_t2
	// - t3: plain table
	// We keep only "t1".
	goldenInput := `/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET NAMES utf8mb4 */;
--
-- Current Database: ` + "`" + `shop` + "`" + `
--

--
-- Table structure for table ` + "`" + `t1` + "`" + `
--

DROP TABLE IF EXISTS ` + "`" + `t1` + "`" + `;
CREATE TABLE ` + "`" + `t1` + "`" + ` (
  ` + "`" + `id` + "`" + ` int NOT NULL,
  ` + "`" + `v` + "`" + ` varchar(50) DEFAULT NULL,
  PRIMARY KEY (` + "`" + `id` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table ` + "`" + `t1` + "`" + `
--

LOCK TABLES ` + "`" + `t1` + "`" + ` WRITE;
/*!40000 ALTER TABLE ` + "`" + `t1` + "`" + ` DISABLE KEYS */;
INSERT INTO ` + "`" + `t1` + "`" + ` VALUES (1,'val1'),(2,'val2');
/*!40000 ALTER TABLE ` + "`" + `t1` + "`" + ` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table ` + "`" + `t2` + "`" + `
--

DROP TABLE IF EXISTS ` + "`" + `t2` + "`" + `;
CREATE TABLE ` + "`" + `t2` + "`" + ` (
  ` + "`" + `id` + "`" + ` int NOT NULL,
  ` + "`" + `ref` + "`" + ` int DEFAULT NULL,
  PRIMARY KEY (` + "`" + `id` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table ` + "`" + `t2` + "`" + `
--

LOCK TABLES ` + "`" + `t2` + "`" + ` WRITE;
/*!40000 ALTER TABLE ` + "`" + `t2` + "`" + ` DISABLE KEYS */;
INSERT INTO ` + "`" + `t2` + "`" + ` VALUES (10,1);
/*!40000 ALTER TABLE ` + "`" + `t2` + "`" + ` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table ` + "`" + `t3` + "`" + `
--

DROP TABLE IF EXISTS ` + "`" + `t3` + "`" + `;
CREATE TABLE ` + "`" + `t3` + "`" + ` (
  ` + "`" + `id` + "`" + ` int NOT NULL,
  ` + "`" + `note` + "`" + ` text,
  PRIMARY KEY (` + "`" + `id` + "`" + `)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table ` + "`" + `t3` + "`" + `
--

LOCK TABLES ` + "`" + `t3` + "`" + ` WRITE;
/*!40000 ALTER TABLE ` + "`" + `t3` + "`" + ` DISABLE KEYS */;
INSERT INTO ` + "`" + `t3` + "`" + ` VALUES (100,'note3');
/*!40000 ALTER TABLE ` + "`" + `t3` + "`" + ` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Temporary view structure for view ` + "`" + `v_t1` + "`" + `
--

DROP TABLE IF EXISTS ` + "`" + `v_t1` + "`" + `;
CREATE TABLE ` + "`" + `v_t1` + "`" + ` (` + "`" + `id` + "`" + ` int);

--
-- Dumping triggers for database 'shop'
--

DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=` + "`" + `root` + "`" + `@` + "`" + `%` + "`" + `*/ /*!50003 TRIGGER ` + "`" + `tr_t1` + "`" + ` AFTER INSERT ON ` + "`" + `t1` + "`" + ` FOR EACH ROW BEGIN UPDATE t1 SET v = NEW.v; END */;;
DELIMITER ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=` + "`" + `root` + "`" + `@` + "`" + `%` + "`" + `*/ /*!50003 TRIGGER ` + "`" + `tr_t2` + "`" + ` AFTER INSERT ON ` + "`" + `t2` + "`" + ` FOR EACH ROW BEGIN UPDATE t2 SET ref = NEW.ref; END */;;
DELIMITER ;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
-- Dump completed on 2026-11-20 23:09:50
`

	filter := NewMySQLPartialFilter(strings.NewReader(goldenInput), []string{"t1"})
	outBytes, err := io.ReadAll(filter)
	if err != nil {
		t.Fatalf("filter error: %v", err)
	}
	out := string(outBytes)

	// Check header and footer kept
	if !strings.Contains(out, "SET @OLD_CHARACTER_SET_CLIENT") {
		t.Errorf("expected header SET statements kept")
	}
	if !strings.Contains(out, "SET SQL_MODE=@OLD_SQL_MODE") || !strings.Contains(out, "Dump completed") {
		t.Errorf("expected footer SET statements kept")
	}

	// Check t1 kept
	if !strings.Contains(out, "CREATE TABLE `t1`") || !strings.Contains(out, "INSERT INTO `t1`") {
		t.Errorf("expected table t1 structure and data kept")
	}

	// Check trigger tr_t1 kept
	if !strings.Contains(out, "TRIGGER `tr_t1`") {
		t.Errorf("expected trigger tr_t1 on table t1 kept")
	}

	// Check t2 and t3 dropped
	if strings.Contains(out, "CREATE TABLE `t2`") || strings.Contains(out, "INSERT INTO `t2`") {
		t.Errorf("expected table t2 dropped, found in output")
	}
	if strings.Contains(out, "CREATE TABLE `t3`") || strings.Contains(out, "INSERT INTO `t3`") {
		t.Errorf("expected table t3 dropped, found in output")
	}

	// Check trigger tr_t2 dropped
	if strings.Contains(out, "TRIGGER `tr_t2`") {
		t.Errorf("expected trigger tr_t2 dropped, found in output")
	}

	// Test missing table detection
	badFilter := NewMySQLPartialFilter(strings.NewReader(goldenInput), []string{"nonexistent_table"})
	_, badErr := io.ReadAll(badFilter)
	if badErr == nil || !strings.Contains(badErr.Error(), "not found in backup") {
		t.Fatalf("expected unknown table error, got: %v", badErr)
	}
}

func TestPGTOCFilterGolden(t *testing.T) {
	// Golden pg_restore -l TOC output:
	// - orders: has sequence orders_id_seq, index idx_orders_uid, constraint orders_pkey, and FK to users
	// - users: has sequence users_id_seq, constraint users_pkey
	// - products: has constraint products_pkey
	// We restore only "orders". The FK to unselected table "users" should be dropped with a warning.
	goldenTOC := `;
; Archive created at 2026-11-20 23:07:35 UTC
;     dbname: postgres
;     TOC Entries: 22
;     Compression: gzip
;     Dump Version: 1.15-0
;     Format: CUSTOM
;
; Selected TOC Entries:
;
6; 2615 16384 SCHEMA - public postgres
218; 1259 16394 TABLE public orders postgres
217; 1259 16393 SEQUENCE public orders_id_seq postgres
3473; 0 0 SEQUENCE OWNED BY public orders_id_seq postgres
216; 1259 16385 TABLE public users postgres
215; 1259 16384 SEQUENCE public users_id_seq postgres
3474; 0 0 SEQUENCE OWNED BY public users_id_seq postgres
220; 1259 16410 TABLE public products postgres
3311; 2604 16397 DEFAULT public orders id postgres
3310; 2604 16388 DEFAULT public users id postgres
3465; 0 16394 TABLE DATA public orders postgres
3463; 0 16385 TABLE DATA public users postgres
3466; 0 16410 TABLE DATA public products postgres
3475; 0 0 SEQUENCE SET public orders_id_seq postgres
3476; 0 0 SEQUENCE SET public users_id_seq postgres
3316; 2606 16399 CONSTRAINT public orders orders_pkey postgres
3313; 2606 16392 CONSTRAINT public users users_pkey postgres
3318; 2606 16412 CONSTRAINT public products products_pkey postgres
3314; 1259 16405 INDEX public idx_orders_uid postgres
3317; 2606 16400 FK CONSTRAINT public orders orders_user_id_fkey postgres
`

	info := PGTOCInfo{
		FKReferences: map[string]string{
			"orders_user_id_fkey": "users",
		},
		SequenceOwners: map[string]string{
			"orders_id_seq": "orders",
			"users_id_seq":  "users",
		},
		IndexTables: map[string]string{
			"idx_orders_uid": "orders",
		},
	}

	filteredBytes, warnings, err := FilterPGTOC([]byte(goldenTOC), []string{"orders"}, info)
	if err != nil {
		t.Fatalf("FilterPGTOC unexpected error: %v", err)
	}
	filtered := string(filteredBytes)

	// Check orders items are kept
	if !strings.Contains(filtered, "TABLE public orders") {
		t.Errorf("expected TABLE public orders kept")
	}
	if !strings.Contains(filtered, "TABLE DATA public orders") {
		t.Errorf("expected TABLE DATA public orders kept")
	}
	if !strings.Contains(filtered, "SEQUENCE public orders_id_seq") {
		t.Errorf("expected SEQUENCE public orders_id_seq kept")
	}
	if !strings.Contains(filtered, "SEQUENCE OWNED BY public orders_id_seq") {
		t.Errorf("expected SEQUENCE OWNED BY public orders_id_seq kept")
	}
	if !strings.Contains(filtered, "SEQUENCE SET public orders_id_seq") {
		t.Errorf("expected SEQUENCE SET public orders_id_seq kept")
	}
	if !strings.Contains(filtered, "CONSTRAINT public orders orders_pkey") {
		t.Errorf("expected CONSTRAINT public orders orders_pkey kept")
	}
	if !strings.Contains(filtered, "INDEX public idx_orders_uid") {
		t.Errorf("expected INDEX public idx_orders_uid kept")
	}

	// Check FK to unselected table "users" is DROPPED
	if strings.Contains(filtered, "FK CONSTRAINT public orders orders_user_id_fkey") {
		t.Errorf("expected FK CONSTRAINT to unselected table users to be dropped, but was kept")
	}
	if len(warnings) == 0 {
		t.Errorf("expected warning for dropped FK constraint, got none")
	} else if !strings.Contains(warnings[0], "orders_user_id_fkey") || !strings.Contains(warnings[0], "users") {
		t.Errorf("unexpected warning message: %s", warnings[0])
	}

	// Check unselected tables users and products are dropped
	if strings.Contains(filtered, "TABLE public users") || strings.Contains(filtered, "TABLE DATA public users") {
		t.Errorf("expected users table dropped")
	}
	if strings.Contains(filtered, "TABLE public products") || strings.Contains(filtered, "TABLE DATA public products") {
		t.Errorf("expected products table dropped")
	}

	// When both orders and users are selected, FK should be KEPT
	filteredBothBytes, warningsBoth, err := FilterPGTOC([]byte(goldenTOC), []string{"orders", "users"}, info)
	if err != nil {
		t.Fatalf("FilterPGTOC both unexpected error: %v", err)
	}
	if !strings.Contains(string(filteredBothBytes), "FK CONSTRAINT public orders orders_user_id_fkey") {
		t.Errorf("expected FK CONSTRAINT kept when both tables are selected")
	}
	if len(warningsBoth) > 0 {
		t.Errorf("expected no warnings when referenced table is selected, got: %v", warningsBoth)
	}

	// Test missing table detection
	_, _, badErr := FilterPGTOC([]byte(goldenTOC), []string{"unknown_table"}, info)
	if badErr == nil || !strings.Contains(badErr.Error(), "not found in backup") {
		t.Fatalf("expected missing table error, got: %v", badErr)
	}
}
