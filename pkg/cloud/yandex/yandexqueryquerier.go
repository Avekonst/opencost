package yandex

import (
	"context"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/ydb-platform/ydb-go-sdk/v3/table"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type YandexQueryQuerier struct {
	YandexQueryConfiguration
	ConnectionStatus cloud.ConnectionStatus
}

func (yqq *YandexQueryQuerier) GetStatus() cloud.ConnectionStatus {
	// initialize status if it has not done so; this can happen if the integration is inactive
	if yqq.ConnectionStatus.String() == "" {
		yqq.ConnectionStatus = cloud.InitialStatus
	}
	return yqq.ConnectionStatus
}

func (yqq *YandexQueryQuerier) Equals(config cloud.Config) bool {
	thatConfig, ok := config.(*YandexQueryQuerier)
	if !ok {
		return false
	}

	return yqq.YandexQueryConfiguration.Equals(&thatConfig.YandexQueryConfiguration)
}

func (yqq *YandexQueryQuerier) Query(ctx context.Context, queryStr string, rowHandler func(result.Result) error) error {
	err := yqq.Validate()

	if err != nil {
		yqq.ConnectionStatus = cloud.InvalidConfiguration
		return err
	}

	db, err := yqq.GetYandexQueryDB(ctx)
	if err != nil {
		yqq.ConnectionStatus = cloud.FailedConnection
		return err
	}
	defer db.Close(ctx)

	var (
		readTx = table.TxControl(
			table.BeginTx(
				table.WithOnlineReadOnly(),
			),
			table.CommitTx(),
		)
	)

	err = db.Table().Do(ctx,
		func(ctx context.Context, s table.Session) (err error) {
			_, res, err := s.Execute(
				ctx,
				readTx,
				queryStr,
				table.NewQueryParameters(),
			)
			if err != nil {
				return err
			}
			defer res.Close()
			for res.NextResultSet(ctx) {
				for res.NextRow() {
					err = rowHandler(res)
					if err != nil {
						return err
					}
				}
			}
			return res.Err()
		},
	)
	return err
}
