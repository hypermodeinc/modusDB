package modusdb

import (
	"fmt"
)

const (
	objQuery = `
    {
      obj(%s) {
        uid
        expand(_all_) {
            uid
            expand(_all_)
            dgraph.type
        }
        dgraph.type
        %s
      }
    }`

	funcUid = `func: uid(%d)`
	funcEq  = `func: eq(%s, %s)`
)

func uidQuery(gid uint64) string {
	return fmt.Sprintf(funcUid, gid)
}

func eqQuery(key, value any) string {
	return fmt.Sprintf(funcEq, key, value)
}

func formatObjQuery(qf string, extraFields string) string {
	return fmt.Sprintf(objQuery, qf, extraFields)
}
