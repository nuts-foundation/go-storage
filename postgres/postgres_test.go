/*
 * Copyright (C) 2022 Nuts community
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/nuts-foundation/go-stoabs"
	"github.com/nuts-foundation/go-stoabs/kvtests"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"testing"
)

func TestPostgres(t *testing.T) {
	container, db, err := startDatabase()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = container.Terminate(context.Background())
	})
	provider := func(t *testing.T) (stoabs.KVStore, error) {
		rows, err := db.Query(
			`SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'`)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var table string
			err = rows.Scan(&table)
			if err != nil {
				return nil, err
			}
			_, err := db.Exec("DROP TABLE " + table)
			if err != nil {
				return nil, err
			}
		}
		return CreatePostgresStore(db)
	}
	kvtests.TestReadingAndWriting(t, provider)
	kvtests.TestClose(t, provider)
	kvtests.TestDelete(t, provider)
	kvtests.TestStats(t, provider)
	kvtests.TestWriteTransactions(t, provider)
	kvtests.TestRange(t, provider)
	kvtests.TestIterate(t, provider)
	// Not sure if we need this
	//kvtests.TestTransactionWriteLock(t, provider)
}

// startDatabase creates a PostgreSQL container and returns a database connection to it.
// See https://dev.to/remast/go-integration-tests-using-testcontainers-9o5
func startDatabase() (testcontainers.Container, *sql.DB, error) {
	ctx := context.Background()
	containerReq := testcontainers.ContainerRequest{
		Image:        "postgres:latest",
		ExposedPorts: []string{"5432/tcp"},
		WaitingFor:   wait.ForListeningPort("5432/tcp"),
		Env: map[string]string{
			"POSTGRES_DB":       "test",
			"POSTGRES_PASSWORD": "postgres",
			"POSTGRES_USER":     "postgres",
		},
	}

	// 2. Start PostgreSQL container
	container, _ := testcontainers.GenericContainer(
		ctx,
		testcontainers.GenericContainerRequest{
			ContainerRequest: containerReq,
			Started:          true,
		})

	host, _ := container.Host(ctx)
	port, _ := container.MappedPort(ctx, "5432")

	dbURI := fmt.Sprintf("postgres://postgres:postgres@%v:%v/test?sslmode=disable", host, port.Port())
	println("Connection string: " + dbURI)
	db, err := sql.Open("postgres", dbURI)
	if err != nil {
		return nil, nil, err
	}
	return container, db, nil
}
