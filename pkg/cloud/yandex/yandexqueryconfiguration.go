package yandex

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/pkg/cloud"
	"github.com/ydb-platform/ydb-go-sdk/v3"
)

type YandexQueryConfiguration struct {
	CloudID          string     `json:"cloudID"`
	ConnectionString string     `json:"connectionString"`
	Table            string     `json:"table"`
	Labels           []string   `json:"labels"`
	Authorizer       Authorizer `json:"authorizer"`
}

func (yqc *YandexQueryConfiguration) GetBillingDataDataset() string {
	return yqc.Table
}

// Key uses the Usage Project Id as the Provider Key for GCP
func (yqc *YandexQueryConfiguration) Key() string {
	return fmt.Sprintf("%s/%s", yqc.CloudID, yqc.GetBillingDataDataset())
}

func (bqc *YandexQueryConfiguration) Provider() string {
	return opencost.YandexProvider
}

// TODO kaverkiev fix after configuration changes
func (yqc *YandexQueryConfiguration) Validate() error {

	if yqc.Authorizer == nil {
		return fmt.Errorf("YandexQueryConfiguration: missing configurer")
	}

	err := yqc.Authorizer.Validate()
	if err != nil {
		return fmt.Errorf("YandexQueryConfiguration: issue with GCP Authorizer: %s", err.Error())
	}

	if yqc.CloudID == "" {
		return fmt.Errorf("YandexQueryConfiguration: missing CloudID")
	}

	if yqc.ConnectionString == "" {
		return fmt.Errorf("YandexQueryConfiguration: missing ConnectionString")
	}

	if yqc.Table == "" {
		return fmt.Errorf("YandexQueryConfiguration: missing Table")
	}

	return nil
}

func (yqc *YandexQueryConfiguration) GetYandexQueryDB(ctx context.Context) (*ydb.Driver, error) {
	opt, err := yqc.Authorizer.CreateYDBOptions()
	if err != nil {
		return nil, err
	}
	db, err := ydb.Open(ctx,
		yqc.ConnectionString,
		opt...,
	)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (yqc *YandexQueryConfiguration) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*YandexQueryConfiguration)
	if !ok {
		return false
	}

	if yqc.Authorizer != nil {
		if !yqc.Authorizer.Equals(thatConfig.Authorizer) {
			return false
		}
	} else {
		if thatConfig.Authorizer != nil {
			return false
		}
	}

	if yqc.CloudID != thatConfig.CloudID {
		return false
	}

	if yqc.ConnectionString != thatConfig.ConnectionString {
		return false
	}

	if yqc.Table != thatConfig.Table {
		return false
	}

	if len(yqc.Labels) != len(thatConfig.Labels) {
		return false
	} else {
		for i, v := range yqc.Labels {
			if v != thatConfig.Labels[i] {
				return false
			}
		}
	}

	return true
}

func (yqc *YandexQueryConfiguration) Sanitize() cloud.Config {
	return &YandexQueryConfiguration{
		CloudID:          yqc.CloudID,
		ConnectionString: yqc.ConnectionString,
		Table:            yqc.Table,
		Authorizer:       yqc.Authorizer.Sanitize().(Authorizer),
	}
}

// UnmarshalJSON assumes data is save as an BigQueryConfigurationDTO
func (yqc *YandexQueryConfiguration) UnmarshalJSON(b []byte) error {
	var f interface{}
	err := json.Unmarshal(b, &f)
	if err != nil {
		return err
	}

	fmap := f.(map[string]interface{})

	cloudID, err := cloud.GetInterfaceValue[string](fmap, "cloudID")
	if err != nil {
		return fmt.Errorf("YandexQueryConfiguration: FromInterface: %s", err.Error())
	}
	yqc.CloudID = cloudID

	connectionString, err := cloud.GetInterfaceValue[string](fmap, "connectionString")
	if err != nil {
		return fmt.Errorf("YandexQueryConfiguration: FromInterface: %s", err.Error())
	}
	yqc.ConnectionString = connectionString

	table, err := cloud.GetInterfaceValue[string](fmap, "table")
	if err != nil {
		return fmt.Errorf("YandexQueryConfiguration: FromInterface: %s", err.Error())
	}
	yqc.Table = table

	labels, err := cloud.GetInterfaceValue[[]interface{}](fmap, "labels")
	if err != nil {
		return fmt.Errorf("YandexQueryConfiguration: FromInterface: %s", err.Error())
	}
	stringLabels := make([]string, len(labels))
	for i, v := range labels {
		str, ok := v.(string)
		if !ok {
			return fmt.Errorf("YandexQueryConfiguration: FromInterface: Labels: %d", i)
		}
		stringLabels[i] = str
	}

	yqc.Labels = stringLabels

	authAny, ok := fmap["authorizer"]
	if !ok {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: missing authorizer")
	}
	authorizer, err := cloud.AuthorizerFromInterface(authAny, SelectAuthorizerByType)
	if err != nil {
		return fmt.Errorf("StorageConfiguration: UnmarshalJSON: %s", err.Error())
	}
	yqc.Authorizer = authorizer
	return nil
}

func (yqc *YandexQueryConfiguration) IsEmpty() bool {
	return yqc.CloudID == "" &&
		yqc.ConnectionString == "" &&
		yqc.Table == "" &&
		len(yqc.Labels) == 0
}
