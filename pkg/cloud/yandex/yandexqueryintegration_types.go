package yandex

import (
	"time"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/timeutil"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
)

type CloudCostLoader struct {
	CloudCost *opencost.CloudCost
}

func (ccl *CloudCostLoader) Load(res result.Result, labels []string) error {
	var (
		start                time.Time
		billing_account_id   string
		billing_account_name *string
		cloud_id             *string
		cloud_name           *string
		folder_id            *string
		service_name         *string
		sku_id               string
		sku_name             *string
		currency             *string
		pricing_quantity     *float64
		pricing_unit         *string
		cost_sum             float64
		credit               float64
		cud_credit           *float64
		total                *float64
	)

	labelsDict := make(map[string]*string)
	for _, key := range labels {
		labelsDict[key] = nil
	}
	var scanArgs []named.Value

	scanArgs = append(scanArgs, named.Required("date", &start))
	scanArgs = append(scanArgs, named.Required("billing_account_id", &billing_account_id))
	scanArgs = append(scanArgs, named.Optional("billing_account_name", &billing_account_name))
	scanArgs = append(scanArgs, named.Optional("cloud_id", &cloud_id))
	scanArgs = append(scanArgs, named.Optional("cloud_name", &cloud_name))
	scanArgs = append(scanArgs, named.Optional("folder_id", &folder_id))
	scanArgs = append(scanArgs, named.Optional("service_name", &service_name))
	scanArgs = append(scanArgs, named.Required("sku_id", &sku_id))
	scanArgs = append(scanArgs, named.Optional("sku_name", &sku_name))
	scanArgs = append(scanArgs, named.Optional("currency", &currency))
	scanArgs = append(scanArgs, named.Optional("pricing_quantity", &pricing_quantity))
	scanArgs = append(scanArgs, named.Optional("pricing_unit", &pricing_unit))
	scanArgs = append(scanArgs, named.Required("cost", &cost_sum))
	scanArgs = append(scanArgs, named.Required("credit", &credit))
	scanArgs = append(scanArgs, named.Optional("cud_credit", &cud_credit))
	scanArgs = append(scanArgs, named.Optional("total", &total))

	for key, value := range labelsDict {
		scanArgs = append(scanArgs, named.Optional(key, &value))
	}

	err := res.ScanNamed(scanArgs...)
	if err != nil {
		return err
	}

	properties := opencost.CloudCostProperties{
		Provider: opencost.YandexProvider,
	}
	var window opencost.Window

	end := start.Add(timeutil.Day)
	window = opencost.NewClosedWindow(start, end)

	properties.InvoiceEntityID = billing_account_id
	if billing_account_name != nil {
		properties.InvoiceEntityName = *billing_account_name
	} else {
		properties.InvoiceEntityName = ""
	}

	if cloud_id != nil {
		properties.AccountID = *cloud_id
	} else {
		properties.AccountID = ""
	}

	if cloud_name != nil {
		properties.AccountName = *cloud_name
	} else {
		properties.AccountName = ""
	}

	if service_name != nil {
		properties.Service = *service_name
	} else {
		properties.Service = ""
	}

	if sku_name != nil {
		properties.Category = SelectCategory(properties.Service, *sku_name)
	} else {
		properties.Category = opencost.ComputeCategory
	}

	if len(labels) > 0 {
		labels := map[string]string{}
		for key := range labelsDict {
			value := labelsDict[key]
			if value != nil {
				labels[key] = *value
			} else {
				labels[key] = ""
			}

		}
		properties.Labels = labels
	}

	ccl.CloudCost = &opencost.CloudCost{
		Properties: &properties,
		Window:     window,
		ListCost: opencost.CostMetric{
			Cost: *total,
		},
	}

	return nil
}
