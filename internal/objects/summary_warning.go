package objects

import (
	"fmt"
	"strings"
)

// SummaryWarning: "N views, M triggers... are not migrated automatically"; rỗng nếu không có gì.
func SummaryWarning(c map[Kind]int, toMySQL bool) string {
	var parts []string
	for _, k := range []Kind{KindView, KindProcedure, KindFunction, KindTrigger, KindEvent} {
		if n := c[k]; n > 0 {
			name := string(k)
			if n != 1 {
				name += "s"
			}
			parts = append(parts, fmt.Sprintf("%d %s", n, name))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	msg := strings.Join(parts, ", ") + " are not migrated automatically (nor are users and grants; counts cover only what the source account can see)"
	if toMySQL {
		return msg + "; use `strand objects list|export|apply`"
	}
	return msg + ": recreate them on the target by hand"
}
