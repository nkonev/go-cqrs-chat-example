package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/golang-migrate/migrate/v4/source/httpfs"
	pgxStd "github.com/jackc/pgx/v4/stdlib"
	proxy "github.com/shogo82148/go-sql-proxy"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.10.0"
	"go.uber.org/fx"
	"log/slog"
	"main.go/config"
	"main.go/logger"
	"net/http"
	"strings"
	"time"
)

func makeLoggingDriver(cfg *config.AppConfig, slogLogger *slog.Logger) driver.Driver {
	return proxy.NewProxyContext(&pgxStd.Driver{}, &proxy.HooksContext{
		PreExec: func(_ context.Context, _ *proxy.Stmt, _ []driver.NamedValue) (interface{}, error) {
			return time.Now(), nil
		},
		PostExec: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, _ driver.Result, err error) error {
			s := fmt.Sprintf("Exec: %s; args = %v (%s)\n", stmt.QueryString, writeNamedValues(args), time.Since(ctx.(time.Time)))
			if cfg.PostgreSQLConfig.PrettyLog {
				fmt.Println("[SQL] trace_id=" + logger.GetTraceId(c) + ": " + s)
			} else {
				logger.LogWithTrace(c, slogLogger).Debug(s)
			}
			return err
		},

		PreQuery: func(c context.Context, stmt *proxy.Stmt, args []driver.NamedValue) (interface{}, error) {
			return time.Now(), nil
		},
		PostQuery: func(c context.Context, ctx interface{}, stmt *proxy.Stmt, args []driver.NamedValue, rows driver.Rows, err error) error {
			s := fmt.Sprintf("Query: %s; args = %v (%s)\n", stmt.QueryString, writeNamedValues(args), time.Since(ctx.(time.Time)))
			if cfg.PostgreSQLConfig.PrettyLog {
				fmt.Println("[SQL] trace_id=" + logger.GetTraceId(c) + ": " + s)
			} else {
				logger.LogWithTrace(c, slogLogger).Debug(s)
			}
			return err
		},
	})
}

func writeNamedValues(args []driver.NamedValue) string {
	sb := strings.Builder{}
	for i, arg := range args {
		if i != 0 {
			sb.WriteString(fmt.Sprintf(", "))
		}
		if len(arg.Name) > 0 {
			sb.WriteString(arg.Name)
			sb.WriteString(":")
		}
		sb.WriteString(fmt.Sprintf("%#v", arg.Value))
	}

	return sb.String()
}

type DB struct {
	*sql.DB
	lgr *slog.Logger
}

type Tx struct {
	*sql.Tx
	lgr *slog.Logger
}

type CommonOperations interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

func (dbR *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbR.DB.QueryContext(ctx, query, args...)
}

func (txR *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return txR.Tx.QueryContext(ctx, query, args...)
}

func (dbR *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbR.DB.QueryRowContext(ctx, query, args...)
}

func (txR *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return txR.Tx.QueryRowContext(ctx, query, args...)
}

func (dbR *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbR.DB.ExecContext(ctx, query, args...)
}

func (txR *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return txR.Tx.ExecContext(ctx, query, args...)
}

// Begin starts and returns a new transaction.
func (db *DB) Begin(ctx context.Context, lgr *slog.Logger) (*Tx, error) {
	if tx, err := db.DB.BeginTx(ctx, nil); err != nil {
		return nil, fmt.Errorf("error during interacting with db: %w", err)
	} else {
		return &Tx{tx, lgr}, nil
	}
}

func (tx *Tx) SafeRollback() {
	if err0 := tx.Rollback(); err0 != nil {
		tx.lgr.Error("Error during rollback tx ", "err", err0)
	}
}

func TransactWithResult[T any](ctx context.Context, db *DB, txFunc func(*Tx) (T, error)) (ret T, err error) {
	tx, err := db.Begin(ctx, db.lgr)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	ret, err = txFunc(tx)
	return ret, err
}

func Transact(ctx context.Context, db *DB, txFunc func(*Tx) error) (err error) {
	tx, err := db.Begin(ctx, db.lgr)
	if err != nil {
		return
	}
	defer func() {
		if p := recover(); p != nil {
			tx.Rollback()
			panic(p) // re-throw panic after Rollback
		} else if err != nil {
			tx.Rollback() // err is non-nil; don't change it
		} else {
			err = tx.Commit() // err is nil; if Commit returns error update err
		}
	}()
	err = txFunc(tx)
	return err
}

func ConfigureDatabase(
	slogLogger *slog.Logger,
	cfg *config.AppConfig,
	tp *sdktrace.TracerProvider,
	lc fx.Lifecycle,
) (*DB, error) {
	dri := makeLoggingDriver(cfg, slogLogger)

	otDriver := otelsql.WrapDriver(dri, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	))

	connector, err := otDriver.(driver.DriverContext).OpenConnector(cfg.PostgreSQLConfig.Url)
	if err != nil {
		return nil, err
	}
	db := sql.OpenDB(connector)

	err = otelsql.RegisterDBStatsMetrics(db, otelsql.WithAttributes(
		semconv.DBSystemPostgreSQL,
	), otelsql.WithTracerProvider(tp))
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(cfg.PostgreSQLConfig.MaxLifetime)
	db.SetMaxIdleConns(cfg.PostgreSQLConfig.MaxIdleConnections)
	db.SetMaxOpenConns(cfg.PostgreSQLConfig.MaxOpenConnections)

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	dbWrapper := &DB{
		DB:  db,
		lgr: slogLogger,
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			slogLogger.Info("Stopping database")
			if err := db.Close(); err != nil {
				slogLogger.Error("Error shutting down database", "err", err)
			}
			return nil
		},
	})

	return dbWrapper, nil
}

//go:embed migrations
var embeddedMigrationFiles embed.FS

func (db *DB) Migrate(mc config.MigrationConfig) error {
	db.lgr.Info("Starting migration")
	staticDir := http.FS(embeddedMigrationFiles)
	src, err := httpfs.New(staticDir, "migrations")
	if err != nil {
		return err
	}

	pgInstance, err := postgres.WithInstance(db.DB, &postgres.Config{
		MigrationsTable:  mc.MigrationTable,
		StatementTimeout: mc.StatementDuration,
	})
	if err != nil {
		return err
	}

	m, err := migrate.NewWithInstance("httpfs", src, "", pgInstance)
	if err != nil {
		return err
	}
	//defer m.Close()
	if err := m.Up(); err != nil && err.Error() != "no change" {
		return err
	}
	db.lgr.Info("Migration successfully completed")
	return nil
}

func (db *DB) Reset(mc config.MigrationConfig) error {
	_, err := db.Exec(fmt.Sprintf(`
	drop sequence if exists chat_id_sequence;
	
	drop table if exists chat_common;
	drop table if exists chat_participant;
	drop table if exists message;
	drop table if exists chat_user_view;
	drop table if exists unread_messages_user_view;
	drop table if exists technical;

	drop table if exists %s;
	
	-- test
`, mc.MigrationTable))
	db.lgr.Info("Recreating database", "err", err)
	return err
}

func RunMigrations(db *DB, cfg *config.AppConfig) error {
	return db.Migrate(cfg.PostgreSQLConfig.MigrationConfig)
}

func RunResetDatabase(db *DB, cfg *config.AppConfig) error {
	return db.Reset(cfg.PostgreSQLConfig.MigrationConfig)
}
