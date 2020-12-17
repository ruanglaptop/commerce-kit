package data

import (
	"context"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
	"github.com/payfazz/commerce-kit/appcontext"
	"github.com/payfazz/commerce-kit/client"
	"github.com/payfazz/commerce-kit/helper"
)

// Manager represents the manager to manage the data consistency
type Manager struct {
	db                 *sqlx.DB
	acknowledgeService client.AcknowledgeRequestServiceInterface
	eventHandler       helper.EventMirroringServiceInterface
}

func (m *Manager) publishQueryModelEvents(ctx *context.Context) {
	currentQueryModelEvents := []*helper.PublishEventParams{}
	temp := appcontext.CurrentQueryModelEvents(ctx)
	if temp != nil {
		currentQueryModelEvents = temp.([]*helper.PublishEventParams)
	}

	tempCurrentAccount := appcontext.CurrentAccount(ctx)
	if tempCurrentAccount == nil {
		defaultValue := 0
		tempCurrentAccount = &defaultValue
	}
	backgroundContext := context.WithValue(context.Background(), appcontext.KeyCurrentAccount, *tempCurrentAccount)

	for _, eventParams := range currentQueryModelEvents {
		errPubsub := m.eventHandler.Publish(
			&backgroundContext,
			eventParams,
		)
		if errPubsub != nil {
			log.Printf(`
				[Failed in Publishing Event]:
					Event: %v
					Error: %v
			`, eventParams, errPubsub)
		}
	}
}

// RunInTransaction runs the f with the transaction queryable inside the context
func (m *Manager) RunInTransaction(ctx *context.Context, f func(tctx *context.Context) error) error {
	tx, err := m.db.Beginx()
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("error when creating transction: %v", err)
	}

	ctx = NewContext(ctx, tx)
	err = m.acknowledgeService.Prepare(ctx)
	if err != nil {
		fmt.Printf("\n[Commerce-Kit - RunInTransaction - Prepare] Error: %v\n", err)
	}
	err = f(ctx)
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
	m.publishQueryModelEvents(ctx)

	return nil
}

// NewManager creates a new manager
func NewManager(
	db *sqlx.DB,
	acknowledgeService client.AcknowledgeRequestServiceInterface,
	eventHandler helper.EventMirroringServiceInterface,
) *Manager {
	return &Manager{
		db:                 db,
		acknowledgeService: acknowledgeService,
		eventHandler:       eventHandler,
	}
}
