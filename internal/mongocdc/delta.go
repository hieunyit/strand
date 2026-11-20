package mongocdc

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// updateDesc is the update description of a change event (updateDescription).
type updateDesc struct {
	UpdatedFields      bson.Raw     `bson:"updatedFields"`
	RemovedFields      []string     `bson:"removedFields"`
	TruncatedArrays    []truncation `bson:"truncatedArrays"`
	DisambiguatedPaths bson.Raw     `bson:"disambiguatedPaths"`
	// exactPaths: the stream reports disambiguatedPaths (6.1+ with expanded events), so
	// unlisted paths contain no field name with '.' (set by the reader, not decoded).
	exactPaths bool
	// noCreate: the source logs update diffs (5.0+) where a new parent is always reported
	// whole, so a missing intermediate document can only mean a field name containing
	// '.' (e.g. added with $setField): ambiguous without exactPaths.
	noCreate bool
}

type truncation struct {
	Field   string `bson:"field"`
	NewSize int32  `bson:"newSize"`
}

// errNeedLookup means an update description cannot be applied exactly to the image at
// hand (ambiguous path, missing or diverged pre-image): the document is re-read from
// the source instead.
var errNeedLookup = errors.New("update description needs a full-document lookup")

// applyUpdate applies an update description to the document image doc (the state of
// the document right before the event) and returns the new image.
//
// Semantics follow MongoDB's update application: truncations, then removed fields, then
// updated fields in the order the event lists them. An updated field that exists keeps
// its position; a new field is appended. A numeric path component addresses an array
// element when the container is an array and a field name otherwise. Setting an array
// index past the end pads with null; a missing intermediate document is created (4.4
// oplog entries report "$set a.b.c" without its parents). Anything MongoDB would reject
// or that is ambiguous (field names containing '.', a disambiguatedPaths entry) returns
// errNeedLookup. Untouched values are copied byte for byte.
func applyUpdate(doc bson.Raw, d *updateDesc) (bson.Raw, error) {
	root, err := parseNode(doc, false)
	if err != nil {
		return nil, err
	}
	// 6.1+ reports disambiguatedPaths for every path containing a field name with '.'
	// or a numeric field name; then (and only then, see exactPaths) a path that is not
	// listed has no such component and splitting on '.' is exact.
	var disamb map[string][]pathComp
	if len(d.DisambiguatedPaths) > 5 {
		if disamb, err = parseDisambiguated(d.DisambiguatedPaths); err != nil {
			return nil, errNeedLookup
		}
	}
	root.trustDots = d.exactPaths
	root.noCreate = d.noCreate && !d.exactPaths
	resolve := func(p string) []pathComp {
		if c, ok := disamb[p]; ok {
			return c
		}
		return splitPath(p)
	}
	for _, t := range d.TruncatedArrays {
		if err := root.truncate(resolve(t.Field), int(t.NewSize)); err != nil {
			return nil, err
		}
	}
	for _, f := range d.RemovedFields {
		if err := root.unset(resolve(f)); err != nil {
			return nil, err
		}
	}
	if len(d.UpdatedFields) > 0 {
		elems, err := bsoncore.Document(d.UpdatedFields).Elements()
		if err != nil {
			return nil, err
		}
		for _, e := range elems {
			v := e.Value()
			if err := root.set(resolve(e.Key()), bson.RawValue{Type: bson.Type(v.Type), Value: v.Data}); err != nil {
				return nil, err
			}
		}
	}
	return root.encode(nil)
}

// pathComp is one path component: a field name, or (typed) an array index.
type pathComp struct {
	key   string
	index int
	typed bool // from disambiguatedPaths: isIndex is authoritative
	isIdx bool
}

func parseDisambiguated(raw bson.Raw) (map[string][]pathComp, error) {
	elems, err := raw.Elements()
	if err != nil {
		return nil, err
	}
	out := make(map[string][]pathComp, len(elems))
	for _, e := range elems {
		arr, ok := e.Value().ArrayOK()
		if !ok {
			return nil, errNeedLookup
		}
		vals, err := arr.Values()
		if err != nil {
			return nil, err
		}
		comps := make([]pathComp, 0, len(vals))
		for _, v := range vals {
			switch v.Type {
			case bson.TypeString:
				comps = append(comps, pathComp{key: v.StringValue(), typed: true})
			case bson.TypeInt32, bson.TypeInt64:
				i, _ := v.AsInt64OK()
				comps = append(comps, pathComp{key: strconv.FormatInt(i, 10), index: int(i), typed: true, isIdx: true})
			default:
				return nil, errNeedLookup
			}
		}
		if len(comps) == 0 {
			return nil, errNeedLookup
		}
		out[e.Key()] = comps
	}
	return out, nil
}

func splitPath(p string) []pathComp {
	parts := strings.Split(p, ".")
	out := make([]pathComp, len(parts))
	for i, k := range parts {
		out[i] = pathComp{key: k}
	}
	return out
}

// node is a lazily expanded BSON document or array.
type node struct {
	array bool
	keys  []string
	vals  []bson.RawValue
	kids  []*node // expanded child (document/array) or nil: use vals
	// dotted: some field name of this document contains '.' or starts with '$'.
	dotted bool
	// trustDots: path components are known not to contain '.' (disambiguated stream).
	trustDots bool
	noCreate  bool
}

func parseNode(raw []byte, array bool) (*node, error) {
	elems, err := bsoncore.Document(raw).Elements()
	if err != nil {
		return nil, fmt.Errorf("decode document image: %w", err)
	}
	n := &node{array: array, keys: make([]string, len(elems)), vals: make([]bson.RawValue, len(elems)), kids: make([]*node, len(elems))}
	for i, e := range elems {
		k := e.Key()
		n.keys[i] = k
		v := e.Value()
		n.vals[i] = bson.RawValue{Type: bson.Type(v.Type), Value: v.Data}
		if !array && (strings.ContainsRune(k, '.') || strings.HasPrefix(k, "$")) {
			n.dotted = true
		}
	}
	return n, nil
}

func (n *node) child(i int) (*node, error) {
	if n.kids[i] != nil {
		return n.kids[i], nil
	}
	v := n.vals[i]
	switch v.Type {
	case bson.TypeEmbeddedDocument, bson.TypeArray:
		c, err := parseNode(v.Value, v.Type == bson.TypeArray)
		if err != nil {
			return nil, err
		}
		c.trustDots, c.noCreate = n.trustDots, n.noCreate
		n.kids[i] = c
		return c, nil
	}
	return nil, nil // scalar
}

// index returns the position of path component c in n, or -1.
func (n *node) index(c pathComp) (int, error) {
	k := c.key
	if c.typed && c.isIdx != n.array {
		return -1, errNeedLookup // the source saw a different container type
	}
	if n.array {
		i, err := arrayIndex(k)
		if err != nil {
			return -1, err
		}
		if i < len(n.keys) {
			return i, nil
		}
		return -1, nil
	}
	for i, key := range n.keys {
		if key == k {
			return i, nil
		}
	}
	return -1, nil
}

// arrayIndex parses a path component addressing an array element (MongoDB accepts only
// plain non-negative decimal integers).
func arrayIndex(k string) (int, error) {
	if k == "" || len(k) > 9 || strings.TrimLeft(k, "0123456789") != "" {
		return 0, errNeedLookup // MongoDB would refuse a field name on an array
	}
	i, err := strconv.Atoi(k)
	if err != nil {
		return 0, errNeedLookup
	}
	return i, nil
}

func (n *node) ambiguous() bool { return !n.array && n.dotted && !n.trustDots }

// walk descends to the container of the last path component. create: create missing
// intermediate documents (set); otherwise a missing intermediate returns (nil, nil).
func (n *node) walk(path []pathComp, create bool) (*node, error) {
	cur := n
	for j, k := range path[:len(path)-1] {
		if cur.ambiguous() {
			return nil, errNeedLookup
		}
		i, err := cur.index(k)
		if err != nil {
			return nil, err
		}
		if i < 0 {
			if !create {
				return nil, nil
			}
			if cur.noCreate {
				return nil, errNeedLookup
			}
			if path[j+1].typed && path[j+1].isIdx {
				return nil, errNeedLookup // would create an array: never reported this way
			}
			child := &node{array: false, trustDots: cur.trustDots, noCreate: cur.noCreate}
			cur.put(k, bson.RawValue{}, child)
			cur = child
			continue
		}
		c, err := cur.child(i)
		if err != nil {
			return nil, err
		}
		if c == nil { // traversing a scalar: MongoDB rejects the update
			if !create && cur.vals[i].Type == bson.TypeNull {
				return nil, nil
			}
			return nil, errNeedLookup
		}
		cur = c
	}
	if cur.ambiguous() {
		return nil, errNeedLookup
	}
	return cur, nil
}

// put sets key c (document) or index c (array) to v / child, appending if absent.
func (n *node) put(c pathComp, v bson.RawValue, child *node) {
	k := c.key
	if n.array {
		i, _ := arrayIndex(k)
		for len(n.keys) < i {
			n.keys = append(n.keys, "")
			n.vals = append(n.vals, bson.RawValue{Type: bson.TypeNull})
			n.kids = append(n.kids, nil)
		}
		if i < len(n.keys) {
			n.vals[i], n.kids[i] = v, child
			return
		}
		n.keys = append(n.keys, "")
		n.vals = append(n.vals, v)
		n.kids = append(n.kids, child)
		return
	}
	for i, key := range n.keys {
		if key == k {
			n.vals[i], n.kids[i] = v, child
			return
		}
	}
	n.keys = append(n.keys, k)
	n.vals = append(n.vals, v)
	n.kids = append(n.kids, child)
	if strings.ContainsRune(k, '.') || strings.HasPrefix(k, "$") {
		n.dotted = true
	}
}

func (n *node) set(path []pathComp, v bson.RawValue) error {
	if len(path) == 1 && path[0].key == "_id" {
		return errNeedLookup // _id is immutable; never expected in an update description
	}
	c, err := n.walk(path, true)
	if err != nil {
		return err
	}
	last := path[len(path)-1]
	if last.typed && last.isIdx != c.array {
		return errNeedLookup
	}
	if c.array {
		if _, err := arrayIndex(last.key); err != nil {
			return err
		}
	}
	c.put(last, v, nil)
	return nil
}

func (n *node) unset(path []pathComp) error {
	c, err := n.walk(path, false)
	if err != nil || c == nil {
		return err
	}
	i, err := c.index(path[len(path)-1])
	if err != nil {
		return err
	}
	if i < 0 {
		return nil
	}
	if c.array { // $unset of an array element sets it to null
		c.vals[i], c.kids[i] = bson.RawValue{Type: bson.TypeNull}, nil
		return nil
	}
	c.keys = append(c.keys[:i], c.keys[i+1:]...)
	c.vals = append(c.vals[:i], c.vals[i+1:]...)
	c.kids = append(c.kids[:i], c.kids[i+1:]...)
	return nil
}

func (n *node) truncate(path []pathComp, size int) error {
	c, err := n.walk(path, false)
	if err != nil {
		return err
	}
	if c == nil {
		return errNeedLookup
	}
	i, err := c.index(path[len(path)-1])
	if err != nil {
		return err
	}
	if i < 0 {
		return errNeedLookup
	}
	arr, err := c.child(i)
	if err != nil {
		return err
	}
	if arr == nil || !arr.array || size < 0 || size > len(arr.keys) {
		return errNeedLookup
	}
	arr.keys, arr.vals, arr.kids = arr.keys[:size], arr.vals[:size], arr.kids[:size]
	return nil
}

func (n *node) encode(dst []byte) ([]byte, error) {
	idx, dst := bsoncore.AppendDocumentStart(dst)
	for i := range n.keys {
		key := n.keys[i]
		if n.array {
			key = strconv.Itoa(i)
		}
		if k := n.kids[i]; k != nil {
			t := bsoncore.TypeEmbeddedDocument
			if k.array {
				t = bsoncore.TypeArray
			}
			dst = bsoncore.AppendHeader(dst, t, key)
			var err error
			if dst, err = k.encode(dst); err != nil {
				return nil, err
			}
			continue
		}
		dst = bsoncore.AppendValueElement(dst, key, bsoncore.Value{Type: bsoncore.Type(n.vals[i].Type), Data: n.vals[i].Value})
	}
	return bsoncore.AppendDocumentEnd(dst, idx)
}
