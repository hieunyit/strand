package schemagen

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"strand/internal/model"
)

func pgToPG(ctx context.Context, r Request, t model.TableID) ([]string, []string, []string, error) {
	dst := r.target(t)
	var oid int64
	var partition string
	err := r.Source.QueryRowContext(ctx, `SELECT c.oid::bigint,COALESCE(pg_get_partkeydef(c.oid),'') FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind IN ('r','p')`, t.Schema, t.Table).Scan(&oid, &partition)
	if err != nil {
		return nil, nil, nil, err
	}
	rows, err := r.Source.QueryContext(ctx, `SELECT a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,COALESCE(pg_get_expr(d.adbin,d.adrelid),''),a.attidentity::text,a.attgenerated::text,COALESCE(cn.nspname,''),COALESCE(co.collname,''),tn.nspname
 FROM pg_attribute a JOIN pg_type ty ON ty.oid=a.atttypid JOIN pg_namespace tn ON tn.oid=ty.typnamespace
 LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 LEFT JOIN pg_collation co ON co.oid=a.attcollation AND a.attcollation<>ty.typcollation LEFT JOIN pg_namespace cn ON cn.oid=co.collnamespace
 WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum`, oid)
	if err != nil {
		return nil, nil, nil, err
	}
	type column struct {
		n, typ, def, identity, generated, collschema, coll, typeschema string
		notnull                                                        bool
	}
	var cols []column
	for rows.Next() {
		var c column
		if err := rows.Scan(&c.n, &c.typ, &c.notnull, &c.def, &c.identity, &c.generated, &c.collschema, &c.coll, &c.typeschema); err != nil {
			rows.Close()
			return nil, nil, nil, err
		}
		cols = append(cols, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, nil, nil, err
	}
	ddl := []string{"CREATE SCHEMA IF NOT EXISTS " + quote(dst.Schema, true)}
	var defs, owned, warnings []string
	for _, c := range cols {
		if c.typeschema != "pg_catalog" {
			return nil, nil, nil, reject("user-defined type %s on %s.%s: create and map the target table manually", c.typ, t, c.n)
		}
		d := quote(c.n, true) + " " + c.typ
		if c.coll != "" {
			d += " COLLATE " + quote(c.collschema, true) + "." + quote(c.coll, true)
		}
		var seqschema, seqname, seqtype string
		var start, inc, min, max, cache int64
		var cycle bool
		err := r.Source.QueryRowContext(ctx, `SELECT sn.nspname,s.relname,format_type(q.seqtypid,NULL),q.seqstart,q.seqincrement,q.seqmin,q.seqmax,q.seqcache,q.seqcycle
 FROM pg_depend dep JOIN pg_class s ON s.oid=dep.objid AND s.relkind='S' JOIN pg_namespace sn ON sn.oid=s.relnamespace JOIN pg_sequence q ON q.seqrelid=s.oid JOIN pg_attribute a ON a.attrelid=dep.refobjid AND a.attnum=dep.refobjsubid
 WHERE dep.classid='pg_class'::regclass AND dep.refclassid='pg_class'::regclass AND dep.refobjid=$1 AND a.attname=$2 AND dep.deptype IN ('a','i')`, oid, c.n).Scan(&seqschema, &seqname, &seqtype, &start, &inc, &min, &max, &cache, &cycle)
		if err != nil && err != sql.ErrNoRows {
			return nil, nil, nil, err
		}
		if err == nil {
			opts := fmt.Sprintf("START WITH %d INCREMENT BY %d MINVALUE %d MAXVALUE %d CACHE %d", start, inc, min, max, cache)
			if cycle {
				opts += " CYCLE"
			} else {
				opts += " NO CYCLE"
			}
			if c.identity != "" {
				mode := "BY DEFAULT"
				if c.identity == "a" {
					mode = "ALWAYS"
				}
				d += " GENERATED " + mode + " AS IDENTITY (" + opts + ")"
			} else {
				seq := model.TableID{Schema: dst.Schema, Table: seqname}
				if dst != t {
					seq.Table = dst.Table + "_" + c.n + "_seq"
				}
				// Kiểu sequence độc lập với kiểu cột sở hữu; dùng seqtypid để giữ nguyên
				// MIN/MAX và không sinh AS integer MAXVALUE của bigint (02 §8).
				ddl = append(ddl, "CREATE SEQUENCE "+name(seq, true)+" AS "+seqtype+" "+opts)
				if c.def != "" {
					def, err := rewriteSequenceDefault(c.def, model.TableID{Schema: seqschema, Table: seqname}, seq)
					if err != nil {
						return nil, nil, nil, err
					}
					d += " DEFAULT " + def
				}
				owned = append(owned, "ALTER SEQUENCE "+name(seq, true)+" OWNED BY "+name(dst, true)+"."+quote(c.n, true))
			}
		} else if c.generated != "" {
			storage := "STORED"
			if c.generated == "v" {
				storage = "VIRTUAL"
			}
			d += " GENERATED ALWAYS AS (" + c.def + ") " + storage
		} else if c.def != "" {
			if strings.Contains(c.def, "nextval(") {
				return nil, nil, nil, reject("unowned sequence default on %s.%s requires manual mapping", t, c.n)
			}
			d += " DEFAULT " + c.def
		}
		if c.notnull {
			d += " NOT NULL"
		}
		defs = append(defs, d)
	}
	constraints, err := r.Source.QueryContext(ctx, `SELECT c.conname,c.contype::text,pg_get_constraintdef(c.oid,true),COALESCE(n.nspname,''),COALESCE(p.relname,'')
 FROM pg_constraint c LEFT JOIN pg_class p ON p.oid=c.confrelid LEFT JOIN pg_namespace n ON n.oid=p.relnamespace
 WHERE c.conrelid=$1 AND c.conparentid=0 ORDER BY c.conname`, oid)
	if err != nil {
		return nil, nil, nil, err
	}
	var foreign []string
	for constraints.Next() {
		var n, typ, def, parentSchema, parentTable string
		if err := constraints.Scan(&n, &typ, &def, &parentSchema, &parentTable); err != nil {
			constraints.Close()
			return nil, nil, nil, err
		}
		if t != dst {
			n = dst.Table + "_" + n
		}
		if len(n) > 63 {
			constraints.Close()
			return nil, nil, nil, reject("mapped constraint name %s exceeds 63 bytes", n)
		}
		if typ == "f" {
			rewritten, err := pgReferences(def, t, dst, r.Rename, model.TableID{Schema: parentSchema, Table: parentTable})
			if err != nil {
				constraints.Close()
				return nil, nil, nil, err
			}
			foreign = append(foreign, "ALTER TABLE "+name(dst, true)+" ADD CONSTRAINT "+quote(n, true)+" "+rewritten)
		} else {
			defs = append(defs, "CONSTRAINT "+quote(n, true)+" "+def)
		}
	}
	constraints.Close()
	if err := constraints.Err(); err != nil {
		return nil, nil, nil, err
	}
	create := "CREATE TABLE " + name(dst, true) + " (\n  " + strings.Join(defs, ",\n  ") + "\n)"
	if partition != "" {
		create += " PARTITION BY " + partition
	}
	ddl = append(ddl, create)
	ddl = append(ddl, owned...)
	indexes, err := r.Source.QueryContext(ctx, `SELECT ic.relname,pg_get_indexdef(i.indexrelid) FROM pg_index i JOIN pg_class ic ON ic.oid=i.indexrelid WHERE i.indrelid=$1 AND NOT EXISTS(SELECT 1 FROM pg_constraint c WHERE c.conindid=i.indexrelid AND c.conrelid=i.indrelid AND c.contype IN ('p','u','x')) ORDER BY ic.relname`, oid)
	if err != nil {
		return nil, nil, nil, err
	}
	for indexes.Next() {
		var n, def string
		if err := indexes.Scan(&n, &def); err != nil {
			indexes.Close()
			return nil, nil, nil, err
		}
		at := strings.Index(def, " USING ")
		if at < 0 {
			indexes.Close()
			return nil, nil, nil, reject("cannot map index %s", n)
		}
		unique := ""
		if strings.HasPrefix(def, "CREATE UNIQUE INDEX ") {
			unique = "UNIQUE "
		}
		if t != dst {
			n = dst.Table + "_" + n
		}
		if len(n) > 63 {
			indexes.Close()
			return nil, nil, nil, reject("mapped index name %s exceeds 63 bytes", n)
		}
		ddl = append(ddl, "CREATE "+unique+"INDEX "+quote(n, true)+" ON "+name(dst, true)+def[at:])
	}
	indexes.Close()
	if err := indexes.Err(); err != nil {
		return nil, nil, nil, err
	}
	// Partition con giữ bound và cây phân vùng; cột/index thừa kế từ bảng cha vừa tạo.
	children, err := r.Source.QueryContext(ctx, `SELECT n.nspname,c.relname,pg_get_expr(c.relpartbound,c.oid),COALESCE(pg_get_partkeydef(c.oid),'') FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE i.inhparent=$1 ORDER BY n.nspname,c.relname`, oid)
	if err != nil {
		return nil, nil, nil, err
	}
	type child struct {
		id         model.TableID
		bound, key string
	}
	var kids []child
	for children.Next() {
		var c child
		if err := children.Scan(&c.id.Schema, &c.id.Table, &c.bound, &c.key); err != nil {
			children.Close()
			return nil, nil, nil, err
		}
		kids = append(kids, c)
	}
	children.Close()
	if err := children.Err(); err != nil {
		return nil, nil, nil, err
	}
	for _, c := range kids {
		if c.key != "" {
			return nil, nil, nil, reject("subpartitioned table %s requires manual schema initialization", c.id)
		}
		cdst := r.target(c.id)
		if c.id.Schema == t.Schema {
			cdst.Schema = dst.Schema
		}
		ddl = append(ddl, "CREATE SCHEMA IF NOT EXISTS "+quote(cdst.Schema, true), "CREATE TABLE "+name(cdst, true)+" PARTITION OF "+name(dst, true)+" "+c.bound)
	}
	return ddl, foreign, warnings, nil
}

func pgReferences(def string, src, dst model.TableID, rename map[string]string, referenced ...model.TableID) (string, error) {
	ts, err := lex(def)
	if err != nil {
		return "", err
	}
	for i, t := range ts {
		if t.keyword(def, "REFERENCES") && i+1 < len(ts) {
			a := ts[i+1]
			ref := model.TableID{Schema: src.Schema, Table: a.value}
			end := a.end
			if i+3 < len(ts) && ts[i+2].value == "." {
				ref.Schema = a.value
				ref.Table = ts[i+3].value
				end = ts[i+3].end
			}
			// confrelid mới là danh tính FK; pretty-print có thể bỏ schema theo search_path.
			// Không suy schema của cha từ schema bảng con (bootstrap phải giữ đúng ràng buộc).
			if len(referenced) > 0 {
				ref = referenced[0]
				if ref.Schema == "" || ref.Table == "" {
					return "", fmt.Errorf("foreign key has no referenced catalog relation")
				}
			}
			if mapped, ok := rename[ref.String()]; ok {
				s, t, _ := strings.Cut(mapped, ".")
				ref = model.TableID{Schema: s, Table: t}

			}
			return def[:a.start] + name(ref, true) + def[end:], nil
		}
	}
	return "", fmt.Errorf("foreign key has no REFERENCES clause")
}

// rewriteSequenceDefault chỉ thay literal regclass của sequence đã biết. Giữ
// nguyên toán tử, hàm, cast và mọi literal thường; không biến nextval(seq)*2
// thành nextval(seq). Không phân tích/đoán giá trị row từ source (02 §8).
func rewriteSequenceDefault(expr string, src, dst model.TableID) (string, error) {
	tokens, err := lex(expr)
	if err != nil {
		return "", err
	}
	var out strings.Builder
	last, matches := 0, 0
	for i, t := range tokens {
		if t.ident || expr[t.start] != '\'' || i+3 >= len(tokens) || tokens[i+1].value != ":" || tokens[i+2].value != ":" {
			continue
		}
		regclass := tokens[i+3].value == "regclass"
		if i+5 < len(tokens) && tokens[i+3].value == "pg_catalog" && tokens[i+4].value == "." && tokens[i+5].value == "regclass" {
			regclass = true
		}
		if !regclass {
			continue
		}
		names, err := lex(t.value)
		if err != nil {
			return "", err
		}
		same := len(names) == 1 && names[0].ident && names[0].value == src.Table
		if len(names) == 3 && names[0].ident && names[1].value == "." && names[2].ident {
			same = names[0].value == src.Schema && names[2].value == src.Table
		}
		if !same {
			continue
		}
		out.WriteString(expr[last:t.start])
		out.WriteString(literal(name(dst, true)))
		last = t.end
		matches++
	}
	if matches == 0 && strings.Contains(expr, "nextval(") {
		return "", reject("default has no canonical reference to owned sequence %s; initialize table manually", src)
	}
	out.WriteString(expr[last:])
	return out.String(), nil
}
