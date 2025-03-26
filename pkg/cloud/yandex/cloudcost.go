package yandex

import (
	"strings"

	"github.com/opencost/opencost/core/pkg/opencost"
)

func IsK8s(labels map[string]string) bool {
	// TODO kaverkiev check labels and if they contain kubernetes return true else false
	return false
}

func SelectCategory(service, description string) string {
	s := strings.ToLower(service)
	d := strings.ToLower(description)

	// Network descriptions
	if strings.Contains(d, "ingress") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "egress") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "ic ip") {
		return opencost.NetworkCategory
	}
	if strings.Contains(d, "load balancer") {
		return opencost.NetworkCategory
	}

	// Storage Descriptions
	if strings.Contains(d, "storage") {
		return opencost.StorageCategory
	}
	if strings.Contains(d, "snapshot") {
		return opencost.StorageCategory
	}

	// Service Defaults
	if strings.Contains(s, "compute") {
		return opencost.ComputeCategory
	}
	if strings.Contains(s, "sql") {
		return opencost.StorageCategory
	}
	if strings.Contains(s, "storage") {
		return opencost.StorageCategory
	}
	if strings.Contains(s, "managed") {
		return opencost.ManagementCategory
	} else if strings.Contains(s, "pub/sub") {
		return opencost.NetworkCategory
	}

	return opencost.OtherCategory
}
