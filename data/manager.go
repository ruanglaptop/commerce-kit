package data

import (
	"context"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/payfazz/commerce-kit/client"
)

// Manager represents the manager to manage the data consistency
type Manager struct {
	db                 *sqlx.DB
	acknowledgeService client.AcknowledgeRequestServiceInterface
}

// RunInTransaction runs the f with the transaction queryable inside the context
func (m *Manager) RunInTransaction(ctx context.Context, f func(tctx *context.Context) error) error {
	tx, err := m.db.Beginx()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error when creating transction: %v", err)
	}

	ctx = NewContext(ctx, tx)
	err = m.acknowledgeService.Prepare(&ctx)
	if err != nil {
		fmt.Printf("\n[Commerce-Kit - RunInTransaction - Prepare] Error: %v\n", err)
	}
	err = f(&ctx)
	if err != nil {
		tx.Rollback()
		m.acknowledgeService.Acknowledge(ctx, "rollback", err.Error())
		return err
	}

	err = tx.Commit()
	if err != nil {
		m.acknowledgeService.Acknowledge(ctx, "rollback", fmt.Sprintf("Error when commiting: %s", err.Error()))
		return fmt.Errorf("error when committing transaction: %v", err)
	}
	m.acknowledgeService.Acknowledge(ctx, "commit", "")

	return nil
}

// NewManager creates a new manager
func NewManager(
	db *sqlx.DB,
	acknowledgeService client.AcknowledgeRequestServiceInterface,
) *Manager {
	return &Manager{
		db:                 db,
		acknowledgeService: acknowledgeService,
	}
}
