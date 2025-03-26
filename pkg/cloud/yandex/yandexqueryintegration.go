package yandex

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
)

type YandexQueryIntegration struct {
	YandexQueryQuerier
}

const (
	UsageDateColumnName          = "date"
	BillingAccountIDColumnName   = "billing_account_id"
	BillingAccountNameColumnName = "billing_account_name"
	CloudIDColumnName            = "cloud_id"
	CloudNameColumnName          = "cloud_name"
	FolderIDColumnName           = "folder_id"
	FolderNameColumnName         = "folder_name"
	ServiceColumnName            = "service_name"
	SKUIDColumnName              = "sku_id"
	SKUNameColumnName            = "sku_name"
	// LabelsColumnName             = "labels" // TODO Kaverkiev it is not clear how to get labels from data now
	ResourceIDNameColumnName  = "resource_id"
	CurrencyColumnName        = "currency"
	PricingQuantityColumnName = "pricing_quantity"
	PricingUnitColumnName     = "pricing_unit"
	CostColumnName            = "cost"
	CreditColumnName          = "credit"
	CudCreditColumnName       = "cud_credit"
	TotalColumnName           = "total"
)

const YandexQueryWhereDateFmt = "`date` BETWEEN DateTime::MakeDate(Datetime(\"%s\")) AND DateTime::MakeDate(Datetime(\"%s\"))"

func (yqi *YandexQueryIntegration) GetCloudCost(start time.Time, end time.Time) (*opencost.CloudCostSetRange, error) {
	// Build Query
	selectColumns := []string{
		fmt.Sprintf("`date` as %s", UsageDateColumnName),
		fmt.Sprintf("`billing_account_id` as %s", BillingAccountIDColumnName),
		fmt.Sprintf("`billing_account_name` as %s", BillingAccountNameColumnName),
		fmt.Sprintf("`cloud_id` as %s", CloudIDColumnName),
		fmt.Sprintf("`cloud_name` as %s", CloudNameColumnName),
		fmt.Sprintf("`folder_id` as %s", FolderIDColumnName),
		fmt.Sprintf("`folder_name` as %s", FolderNameColumnName),
		fmt.Sprintf("`service_name` as %s", ServiceColumnName),
		fmt.Sprintf("`sku_id` as %s", SKUIDColumnName),
		fmt.Sprintf("`sku_name` as %s", SKUNameColumnName),
		fmt.Sprintf("`resource_id` as %s", ResourceIDNameColumnName),
		fmt.Sprintf("`currency` as %s", CurrencyColumnName),
		fmt.Sprintf("`pricing_quantity` as %s", PricingQuantityColumnName),
		fmt.Sprintf("`pricing_unit` as %s", PricingUnitColumnName),
		fmt.Sprintf("sum(`cud_credit`) as %s", CudCreditColumnName),
		fmt.Sprintf("sum(`cost`) as %s", CostColumnName),
		fmt.Sprintf("sum(`credit`) as %s", CreditColumnName),
		fmt.Sprintf("sum(`cost` + `credit`) as %s", TotalColumnName),
	}

	for _, label := range yqi.YandexQueryConfiguration.Labels {
		selectColumns = append(selectColumns, fmt.Sprintf("`%s`", label))
	}

	groupByColumns := []string{
		UsageDateColumnName,
		BillingAccountIDColumnName,
		BillingAccountNameColumnName,
		CloudIDColumnName,
		CloudNameColumnName,
		FolderIDColumnName,
		FolderNameColumnName,
		ServiceColumnName,
		SKUIDColumnName,
		SKUNameColumnName,
		ResourceIDNameColumnName,
		CurrencyColumnName,
		PricingQuantityColumnName,
		PricingUnitColumnName,
	}

	for _, label := range yqi.YandexQueryConfiguration.Labels {
		groupByColumns = append(groupByColumns, fmt.Sprintf("`%s`", label))
	}

	columnStr := strings.Join(selectColumns, ", ")
	table := fmt.Sprintf(" `%s` bd ", yqi.GetBillingDataDataset())
	whereClause := fmt.Sprintf(YandexQueryWhereDateFmt, start.Format("2006-01-02T15:04:05Z"), end.Format("2006-01-02T15:04:05Z"))
	groupByStr := strings.Join(groupByColumns, ", ")
	queryStr := `
		SELECT %s
		FROM %s
		WHERE %s
		GROUP BY %s
	`

	querystr := fmt.Sprintf(queryStr, columnStr, table, whereClause, groupByStr)

	// Perform Query and parse values

	ccsr, err := opencost.NewCloudCostSetRange(start, end, opencost.AccumulateOptionDay, yqi.Key())
	if err != nil {
		return ccsr, fmt.Errorf("error creating new CloudCostSetRange: %s", err)
	}

	err = yqi.Query(context.Background(), querystr, func(res result.Result) error {
		ccl := CloudCostLoader{}
		err := ccl.Load(res, yqi.YandexQueryConfiguration.Labels)
		if err != nil {
			return err
		}
		if ccl.CloudCost == nil {
			return nil
		}
		ccsr.LoadCloudCost(ccl.CloudCost)
		return nil
	})
	if err != nil {
		return ccsr, fmt.Errorf("error querying: %s", err)
	}

	return ccsr, nil
}
