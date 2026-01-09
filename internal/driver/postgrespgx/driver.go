package postgrespgx

import (
	"database/sql"

	"github.com/jackc/pgx/v4/stdlib"
)

func init() {
	// Register pgx under the postgres driver name so existing configurations
	// using `postgres` continue to work with the pgx stdlib driver.
	for _, name := range sql.Drivers() {
		if name == "postgres" {
			return
		}
	}
	sql.Register("postgres", stdlib.GetDefaultDriver())
}
