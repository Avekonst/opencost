package yandex

import (
	"encoding/json"
	"fmt"

	"github.com/opencost/opencost/pkg/cloud"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	yc "github.com/ydb-platform/ydb-go-yc"
)

const ServiceAccountKeyAuthorizerType = "YCServiceAccountKey"

type Authorizer interface {
	cloud.Authorizer
	CreateYDBOptions() ([]ydb.Option, error)
}

// SelectAuthorizerByType is an implementation of AuthorizerSelectorFn and acts as a register for Authorizer types
func SelectAuthorizerByType(typeStr string) (Authorizer, error) {
	switch typeStr {
	case ServiceAccountKeyAuthorizerType:
		return &ServiceAccountKey{}, nil
	default:
		return nil, fmt.Errorf("YC: provider authorizer type '%s' is not valid", typeStr)
	}
}

type ServiceAccountKey struct {
	Key map[string]string `json:"key"`
}

// MarshalJSON custom json marshalling functions, sets properties as tagged in struct and sets the authorizer type property
func (ycc *ServiceAccountKey) MarshalJSON() ([]byte, error) {
	fmap := make(map[string]any, 2)
	fmap[cloud.AuthorizerTypeProperty] = ServiceAccountKeyAuthorizerType
	fmap["key"] = ycc.Key
	return json.Marshal(fmap)
}

func (ycc *ServiceAccountKey) Validate() error {
	if ycc.Key == nil || len(ycc.Key) == 0 {
		return fmt.Errorf("ServiceAccountKey: missing Key")
	}

	return nil
}

func (ycc *ServiceAccountKey) Equals(config cloud.Config) bool {
	if config == nil {
		return false
	}
	thatConfig, ok := config.(*ServiceAccountKey)
	if !ok {
		return false
	}

	if len(ycc.Key) != len(thatConfig.Key) {
		return false
	}

	for k, v := range ycc.Key {
		if thatConfig.Key[k] != v {
			return false
		}
	}

	return true
}

func (ycc *ServiceAccountKey) Sanitize() cloud.Config {
	redactedMap := make(map[string]string, len(ycc.Key))
	for key, _ := range ycc.Key {
		redactedMap[key] = cloud.Redacted
	}
	return &ServiceAccountKey{
		Key: redactedMap,
	}
}

func (ycc *ServiceAccountKey) CreateYDBOptions() ([]ydb.Option, error) {
	err := ycc.Validate()
	if err != nil {
		return nil, err
	}

	serviceAccountKey, err := json.Marshal(ycc.Key)
	if err != nil {
		return nil, fmt.Errorf("Key: failed to marshal Key: %s", err.Error())
	}
	client, err := yc.NewClient(
		yc.WithServiceKey(string(serviceAccountKey)),
	)
	if err != nil {
		return nil, err
	}
	opts := ydb.WithCredentials(client)
	return []ydb.Option{opts}, nil
}
