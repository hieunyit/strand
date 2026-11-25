package runner

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"strand/internal/backup"
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

func TestPGTOCFilterP1UnqualifiedSchemaSeparation(t *testing.T) {
	// P1: an unqualified name 'orders' must select ONLY public.orders, NOT sales.orders
	toc := `;
; Selected TOC Entries:
;
6; 2615 16384 SCHEMA - public postgres
7; 2615 16385 SCHEMA - sales postgres
218; 1259 16394 TABLE public orders postgres
219; 1259 16395 TABLE sales orders postgres
3465; 0 16394 TABLE DATA public orders postgres
3466; 0 16395 TABLE DATA sales orders postgres
3316; 2606 16399 CONSTRAINT public orders orders_pkey postgres
3317; 2606 16400 CONSTRAINT sales orders sales_orders_pkey postgres
3318; 2606 16401 TRIGGER public orders tr_orders postgres
3319; 2606 16402 TRIGGER sales orders sales_tr_orders postgres
`
	filteredBytes, _, err := FilterPGTOC([]byte(toc), []string{"orders"}, PGTOCInfo{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	filtered := string(filteredBytes)
	if !strings.Contains(filtered, "TABLE public orders") {
		t.Errorf("expected public.orders kept")
	}
	if !strings.Contains(filtered, "SCHEMA - public") {
		t.Errorf("expected SCHEMA - public kept")
	}
	// Verify sales.* is NOT kept
	if strings.Contains(filtered, "sales") {
		t.Errorf("expected no sales.* lines kept, but got:\n%s", filtered)
	}
}

func TestPGTOCFilterP2NameGuessingAndQuotedIdentifiers(t *testing.T) {
	goldenDDL := `
--
-- Name: orders; Type: TABLE; Schema: public; Owner: postgres
--
CREATE TABLE public.orders (
    id integer NOT NULL,
    customer_id integer,
    created_at timestamp
);

--
-- Name: orders_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--
CREATE SEQUENCE public.orders_id_seq;

--
-- Name: orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--
ALTER SEQUENCE public.orders_id_seq OWNED BY public.orders.id;

--
-- Name: idx_orders_created; Type: INDEX; Schema: public; Owner: postgres
--
CREATE INDEX idx_orders_created ON public.orders USING btree (created_at);

--
-- Name: orders_archive; Type: TABLE; Schema: public; Owner: postgres
--
CREATE TABLE public.orders_archive (
    id integer NOT NULL,
    created_at timestamp
);

--
-- Name: orders_archive_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--
CREATE SEQUENCE public.orders_archive_id_seq;

--
-- Name: orders_archive_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--
ALTER SEQUENCE public.orders_archive_id_seq OWNED BY public.orders_archive.id;

--
-- Name: idx_orders_archive_created; Type: INDEX; Schema: public; Owner: postgres
--
CREATE INDEX idx_orders_archive_created ON public.orders_archive USING btree (created_at);

--
-- Name: Customers; Type: TABLE; Schema: public; Owner: postgres
--
CREATE TABLE public."Customers" (
    "id" integer NOT NULL,
    "email" text
);

--
-- Name: Customers_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--
CREATE SEQUENCE public."Customers_id_seq";

--
-- Name: Customers_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--
ALTER SEQUENCE public."Customers_id_seq" OWNED BY public."Customers"."id";

--
-- Name: idx_Customers_email; Type: INDEX; Schema: public; Owner: postgres
--
CREATE INDEX "idx_Customers_email" ON public."Customers" USING btree ("email");

--
-- Name: invoices; Type: TABLE; Schema: public; Owner: postgres
--
CREATE TABLE public.invoices (
    id integer NOT NULL,
    other_id integer
);

--
-- Name: other_table; Type: TABLE; Schema: public; Owner: postgres
--
CREATE TABLE public.other_table (
    id integer NOT NULL
);

--
-- Name: orders fk_account; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--
ALTER TABLE ONLY public.orders
    ADD CONSTRAINT fk_account FOREIGN KEY (customer_id) REFERENCES public."Customers"(id);

--
-- Name: invoices fk_account; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--
ALTER TABLE ONLY public.invoices
    ADD CONSTRAINT fk_account FOREIGN KEY (other_id) REFERENCES public.other_table(id);
`

	goldenTOC := `;
; Selected TOC Entries:
;
6; 2615 16384 SCHEMA - public postgres
218; 1259 16394 TABLE public orders postgres
217; 1259 16393 SEQUENCE public orders_id_seq postgres
3473; 0 0 SEQUENCE OWNED BY public orders_id_seq postgres
3314; 1259 16405 INDEX public idx_orders_created postgres
3465; 0 16394 TABLE DATA public orders postgres

220; 1259 16410 TABLE public orders_archive postgres
221; 1259 16411 SEQUENCE public orders_archive_id_seq postgres
3474; 0 0 SEQUENCE OWNED BY public orders_archive_id_seq postgres
3315; 1259 16412 INDEX public idx_orders_archive_created postgres
3466; 0 16410 TABLE DATA public orders_archive postgres

222; 1259 16420 TABLE public Customers postgres
223; 1259 16421 SEQUENCE public Customers_id_seq postgres
3475; 0 0 SEQUENCE OWNED BY public Customers_id_seq postgres
3316; 1259 16422 INDEX public idx_Customers_email postgres
3467; 0 16420 TABLE DATA public Customers postgres

224; 1259 16430 TABLE public invoices postgres
225; 1259 16431 TABLE public other_table postgres

3317; 2606 16440 FK CONSTRAINT public orders fk_account postgres
3318; 2606 16441 FK CONSTRAINT public invoices fk_account postgres
`
	info, err := parsePGSchemaMetadata([]byte(goldenDDL))
	if err != nil {
		t.Fatalf("parsePGSchemaMetadata error: %v", err)
	}

	// Selecting orders only:
	// - must keep orders, orders_id_seq, idx_orders_created, orders TABLE DATA
	// - must NOT keep idx_orders_archive_created or orders_archive_id_seq
	// - must NOT keep Customers or invoices or other_table
	filteredBytes, _, err := FilterPGTOC([]byte(goldenTOC), []string{"orders"}, info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	filtered := string(filteredBytes)
	if !strings.Contains(filtered, "TABLE public orders") {
		t.Errorf("expected TABLE public orders kept")
	}
	if !strings.Contains(filtered, "SEQUENCE public orders_id_seq") {
		t.Errorf("expected SEQUENCE public orders_id_seq kept")
	}
	if !strings.Contains(filtered, "INDEX public idx_orders_created") {
		t.Errorf("expected INDEX public idx_orders_created kept")
	}

	// Crucial P2 check 1: when info is empty (metadata has no entry for an object),
	// fallback guessing must NOT keep orders_archive objects when restoring orders!
	filteredNoInfoBytes, _, err := FilterPGTOC([]byte(goldenTOC), []string{"orders"}, PGTOCInfo{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	filteredNoInfo := string(filteredNoInfoBytes)
	if strings.Contains(filteredNoInfo, "idx_orders_archive_created") {
		t.Errorf("P2 violation: fallback guessing kept idx_orders_archive_created when info was empty")
	}
	if strings.Contains(filteredNoInfo, "orders_archive_id_seq") {
		t.Errorf("P2 violation: fallback guessing kept orders_archive_id_seq when info was empty")
	}

	// Crucial P2 check 2: orders_archive objects must NOT be kept with parsed info either!
	if strings.Contains(filtered, "idx_orders_archive_created") {
		t.Errorf("P2 violation: idx_orders_archive_created was kept when restoring orders")
	}
	if strings.Contains(filtered, "orders_archive_id_seq") {
		t.Errorf("P2 violation: orders_archive_id_seq was kept when restoring orders")
	}

	// Selecting orders and Customers:
	// orders fk_account references Customers -> must be kept!
	// invoices fk_account references other_table -> must be dropped!
	filteredBoth, _, err := FilterPGTOC([]byte(goldenTOC), []string{"orders", "Customers"}, info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	filteredBothStr := string(filteredBoth)
	if !strings.Contains(filteredBothStr, "FK CONSTRAINT public orders fk_account") {
		t.Errorf("expected orders fk_account kept when Customers is selected")
	}
	if strings.Contains(filteredBothStr, "FK CONSTRAINT public invoices fk_account") {
		t.Errorf("expected invoices fk_account dropped")
	}

	// Test unresolved PK index error: if orders has an unresolved PK index, fail clearly
	tocWithUnresolvedPK := `;
218; 1259 16394 TABLE public orders postgres
3314; 1259 16405 INDEX public orders_pkey postgres
`
	_, _, pkErr := FilterPGTOC([]byte(tocWithUnresolvedPK), []string{"orders"}, PGTOCInfo{})
	if pkErr == nil || !strings.Contains(pkErr.Error(), "primary key index") {
		t.Fatalf("expected unresolved PK index error, got: %v", pkErr)
	}
}

func TestP3StoreObjectsCap(t *testing.T) {
	// P3: store object list only if <= 1000 names and <= 64 KiB, else store objects_truncated=1
	// Case 1: 1500 names -> not stored, flag set
	details := map[string]string{}
	names1500 := make([]string, 1500)
	for i := range names1500 {
		names1500[i] = fmt.Sprintf("tbl_%d", i)
	}
	storeBackupObjects(details, names1500)
	if details["objects"] != "" {
		t.Errorf("expected objects not stored for 1500 names, got: %s", details["objects"][:50]+"...")
	}
	if details["objects_truncated"] != "1" {
		t.Errorf("expected objects_truncated='1' for 1500 names, got: %q", details["objects_truncated"])
	}

	// Case 2: 500 names exceeding 64 KiB
	details2 := map[string]string{}
	namesBig := make([]string, 500)
	longName := strings.Repeat("a", 200)
	for i := range namesBig {
		namesBig[i] = fmt.Sprintf("%s_%d", longName, i)
	}
	storeBackupObjects(details2, namesBig)
	if details2["objects"] != "" {
		t.Errorf("expected objects not stored for >64KiB list, got len %d", len(details2["objects"]))
	}
	if details2["objects_truncated"] != "1" {
		t.Errorf("expected objects_truncated='1' for >64KiB list, got: %q", details2["objects_truncated"])
	}

	// Case 3: 3 names under limits -> stored normally
	details3 := map[string]string{}
	namesNormal := []string{"users", "orders", "products"}
	storeBackupObjects(details3, namesNormal)
	if details3["objects"] != "users,orders,products" {
		t.Errorf("expected objects stored normally, got: %q", details3["objects"])
	}
	if details3["objects_truncated"] != "" {
		t.Errorf("expected objects_truncated empty, got: %q", details3["objects_truncated"])
	}
}

func TestMySQLRestoreUnknownTableWithKnownListPreValidation(t *testing.T) {
	e := &Env{}
	base := backup.Record{
		Details: map[string]string{
			"databases": "shop",
			"objects":   "tbl1,tbl2",
		},
	}
	plan := RestorePlan{Chain: []backup.Record{base}}
	target := RestoreTarget{
		Database: "shop_restored",
		Objects:  []string{"unknown_tbl"},
	}
	err := e.restoreMySQL(context.Background(), plan, target)
	if err == nil || !strings.Contains(err.Error(), `table "unknown_tbl" not found in backup`) {
		t.Fatalf("expected table not found error before DB creation, got: %v", err)
	}
}

// In a multi-database dump a table name is only unique within its database: selecting "a.t" keeps t of
// database a only, and a bare "t" that exists in several databases is refused as ambiguous.
func TestMySQLPartialFilterMatchesDatabaseAndTable(t *testing.T) {
	q := func(s string) string { return "`" + s + "`" }
	section := func(db string, tables ...string) string {
		var b strings.Builder
		b.WriteString("--\n-- Current Database: " + q(db) + "\n--\n\n")
		b.WriteString("CREATE DATABASE /*!32312 IF NOT EXISTS*/ " + q(db) + ";\n\nUSE " + q(db) + ";\n\n")
		for _, tbl := range tables {
			b.WriteString("--\n-- Table structure for table " + q(tbl) + "\n--\n\n")
			b.WriteString("CREATE TABLE " + q(tbl) + " (id int) /* " + db + "." + tbl + " */;\n\n")
			b.WriteString("--\n-- Dumping data for table " + q(tbl) + "\n--\n\n")
			b.WriteString("INSERT INTO " + q(tbl) + " VALUES (1) /* rows of " + db + "." + tbl + " */;\n\n")
		}
		b.WriteString("--\n-- Dumping triggers for database '" + db + "'\n--\n\n")
		for _, tbl := range tables {
			b.WriteString("DELIMITER ;;\n/*!50003 CREATE*/ /*!50003 TRIGGER " + q("tr_"+db+"_"+tbl) + " AFTER INSERT ON " + q(tbl) +
				" FOR EACH ROW BEGIN END */;;\nDELIMITER ;\n")
		}
		return b.String()
	}
	dump := "/*!40101 SET NAMES utf8mb4 */;\n" + section("a", "t", "u") + section("b", "t", "v") + "-- Dump completed\n"
	run := func(objects ...string) (string, error) {
		f := NewMySQLPartialFilter(strings.NewReader(dump), objects)
		out, err := io.ReadAll(f)
		return string(out), err
	}

	out, err := run("a.t")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "/* a.t */") || !strings.Contains(out, "rows of a.t") || !strings.Contains(out, "tr_a_t") {
		t.Fatalf("a.t not kept:\n%s", out)
	}
	for _, other := range []string{"/* b.t */", "rows of b.t", "tr_b_t", "/* a.u */", "/* b.v */"} {
		if strings.Contains(out, other) {
			t.Fatalf("selecting a.t kept %q:\n%s", other, out)
		}
	}

	out, err = run("v")
	if err != nil || !strings.Contains(out, "rows of b.v") || strings.Contains(out, "rows of a.t") {
		t.Fatalf("an unambiguous bare name: %v\n%s", err, out)
	}
	if _, err := run("t"); err == nil || !strings.Contains(err.Error(), "several databases") {
		t.Fatalf("an ambiguous bare name must be refused: %v", err)
	}
	if _, err := run("a.v"); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("a table of another database must not satisfy a qualified name: %v", err)
	}

	// A single-database dump has no Current Database header: a qualified name matches its database.
	single := strings.Replace(section("shop", "t", "u"), "--\n-- Current Database: `shop`\n--\n", "", 1)
	f := NewMySQLPartialFilterIn(strings.NewReader(single), "shop", []string{"shop.t"})
	b, err := io.ReadAll(f)
	if err != nil || !strings.Contains(string(b), "rows of shop.t") || strings.Contains(string(b), "rows of shop.u") {
		t.Fatalf("qualified selection in a single-database dump: %v\n%s", err, b)
	}
}
