package yandex

import (
	"context"
	"crypto/rsa"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/golang-jwt/jwt/v4"
	"github.com/jszwec/csvutil"
	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/core/pkg/opencost"
	"github.com/opencost/opencost/core/pkg/util/fileutil"
	"github.com/opencost/opencost/pkg/cloud/models"
	"github.com/opencost/opencost/pkg/clustercache"
	"github.com/opencost/opencost/pkg/env"
	"github.com/patrickmn/go-cache"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/billing/v1"
	"github.com/yandex-cloud/go-genproto/yandex/cloud/k8s/v1"
	ycsdk "github.com/yandex-cloud/go-sdk"

	"k8s.io/client-go/metadata"
)

var ycRegions = []string{
	"ru-central1-a",
	"ru-central1-b",
	"ru-central1-c",
	"ru-central1-d",
}

var skuCache = cache.New(5*time.Hour, 10*time.Hour)

const (
	COMPUTE_CLOUD_SERVICE_ID     = "dn22pas77ftg9h3f2djj"
	COMPUTE_CLOUD_GPU_SERVICE_ID = "dn28okfvqh19eiue6l2m"
	GPU_STANDART_V1              = "gpu-standard-v1"
	GPU_STANDART_V1_NAME         = "Intel Broadwell with NVIDIA Tesla V100"
	GPU_STANDART_V2              = "gpu-standard-v2"
	GPU_STANDART_V2_NAME         = "Intel Cascade Lake with NVIDIA Tesla V100"
	GPU_STANDART_V3              = "gpu-standard-v3"
	GPU_STANDART_V3_NAME         = "AMD EPYC with NVIDIA A100"
	STANDART_V3_T4I              = "standard-v3-t4i"
	STANDART_V3_T4I_NAME         = "Intel Ice Lake with t4i"
	HIGHFREQ_V3                  = "highfreq-v3"
	HIGHFREQ_V3_NAME             = "Intel Ice Lake with t4i"
	STANDART_V1                  = "standard-v1"
	STANDART_V1_NAME             = "Intel Broadwell"
	STANDART_V2                  = "standard-v2"
	STANDART_V2_NAME             = "Intel Cascade Lake"
	STANDART_V3                  = "standard-v3"
	STANDART_V3_NAME             = "Intel Ice Lake"
	STANDART_V3_T4               = "standard-v3-t4"
	STANDART_V3_T4_NAME          = "Intel Ice Lake with NVIDIA T4"
	NETWORK_SSD                  = "network-ssd"
	NETWORK_HDD                  = "network-hdd"
	NETWORK_SSD_NONREPLICATED    = "network-ssd-nonreplicated"
	NETWORK_SSD_IO_M3            = "network-ssd-io-m3"
)

var platforms = map[string]string{
	GPU_STANDART_V1: GPU_STANDART_V1_NAME,
	GPU_STANDART_V2: GPU_STANDART_V2_NAME,
	GPU_STANDART_V3: GPU_STANDART_V3_NAME,
	STANDART_V3_T4I: STANDART_V3_T4I_NAME,
	HIGHFREQ_V3:     HIGHFREQ_V3_NAME,
	STANDART_V1:     STANDART_V1_NAME,
	STANDART_V2:     STANDART_V2_NAME,
	STANDART_V3:     STANDART_V3_NAME,
	STANDART_V3_T4:  STANDART_V3_T4_NAME,
}

var storagesCosts = map[string]string{}
var gpuPlatforms = []string{GPU_STANDART_V1, GPU_STANDART_V2, GPU_STANDART_V3, STANDART_V3_T4I, STANDART_V3_T4}
var cpuPlatforms = []string{HIGHFREQ_V3, STANDART_V1, STANDART_V2, STANDART_V3}

type Yandex struct {
	Pricing                 map[string]*YandexPricing
	PVTypeByStorageClass    map[string]string
	CSVLocation             string
	Config                  models.ProviderConfig
	ClusterAccountId        string
	ClusterRegion           string
	DownloadPricingDataLock sync.RWMutex
	Clientset               clustercache.ClusterCache
	YCServiceKey            *ycServiceKey
	MetadataClient          *metadata.Client
	ServiceAccountChecks    *models.ServiceAccountChecks
}

type YandexPricing struct {
	Node *models.Node `json:"node"`
	PV   *models.PV   `json:"pv"`
}

type sku struct {
	InstanceType string `csv:"InstanceType"`
	InstanceID   string `csv:"InstanceID"`
	SKUId        string `csv:"SKUId"`
}

type ycServiceKey struct {
	KeyID            string `json:"id"`
	ServiceAccountID string `json:"service_account_id"`
	CreatedAt        string `json:"created_at"`
	KeyAlgorithm     string `json:"key_algorithm"`
	PublicKey        string `json:"public_key"`
	PrivateKey       string `json:"private_key"`
}

type ycKey struct {
	Labels map[string]string
}

func (k *ycKey) GPUCount() int {
	return 0
}

func (cpk *ycKey) GPUType() string {
	return ""
}

func (cpk *ycKey) ID() string {
	return cpk.Labels["yandex.cloud/node-group-id"]
}

func (cpk *ycKey) Features() string {
	return "default"
}

type ycPVKey struct {
	Labels                 map[string]string
	StorageClassName       string
	StorageClassParameters map[string]string
}

func (key *ycPVKey) ID() string {
	id, ok := key.StorageClassParameters["type"]
	if ok {
		return id
	}
	return key.StorageClassName
}

func (key *ycPVKey) GetStorageClass() string {
	return key.StorageClassName
}

func (key *ycPVKey) Features() string {
	return "default"
}

func (yc *Yandex) ClusterInfo() (map[string]string, error) {
	remoteEnabled := env.IsRemoteEnabled()

	m := make(map[string]string)
	m["name"] = "Yandex Cluster #1"
	c, err := yc.GetConfig()
	if err != nil {
		return nil, err
	}
	if c.ClusterName != "" {
		m["name"] = c.ClusterName
	}
	m["provider"] = opencost.YandexProvider
	m["account"] = yc.ClusterAccountId
	m["region"] = yc.ClusterRegion
	m["remoteReadEnabled"] = strconv.FormatBool(remoteEnabled)
	m["id"] = env.GetClusterID()
	return m, nil
}

func (yc *Yandex) GetAddresses() ([]byte, error) {
	return nil, nil
}

func (yc *Yandex) GetDisks() ([]byte, error) {
	return nil, nil
}

func (yc *Yandex) GetOrphanedResources() ([]models.OrphanedResource, error) {
	return nil, errors.New("not implemented")
}

func (yc *Yandex) NodePricing(key models.Key) (*models.Node, models.PricingMetadata, error) {
	yc.DownloadPricingDataLock.RLock()
	defer yc.DownloadPricingDataLock.RUnlock()

	// Get node features for the key
	nodeGroupId := key.ID()
	pricing, ok := yc.Pricing[nodeGroupId]
	meta := models.PricingMetadata{}
	if !ok {
		log.Errorf("Node pricing information not found for node with Id: %s", nodeGroupId)
		return nil, meta, fmt.Errorf("node pricing information not found for node with Id: %s letting it use default values", nodeGroupId)
	}

	log.Debugf("returning the node price for the node with Id: %s", nodeGroupId)
	returnNode := pricing.Node

	return returnNode, meta, nil
}

func (yc *Yandex) GpuPricing(nodeLabels map[string]string) (string, error) {
	return "", nil
}

func (yc *Yandex) PVPricing(pvk models.PVKey) (*models.PV, error) {
	yc.DownloadPricingDataLock.RLock()
	defer yc.DownloadPricingDataLock.RUnlock()
	pricing, ok := yc.Pricing[pvk.ID()]
	if !ok {
		log.Infof("Persistent Volume pricing not found for %s: %s", pvk.GetStorageClass(), pvk.ID())
		return &models.PV{}, nil
	}
	return pricing.PV, nil
}

// TODO review
func (yc *Yandex) NetworkPricing() (*models.Network, error) {
	cpricing, err := yc.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	znec, err := strconv.ParseFloat(cpricing.ZoneNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	rnec, err := strconv.ParseFloat(cpricing.RegionNetworkEgress, 64)
	if err != nil {
		return nil, err
	}
	inec, err := strconv.ParseFloat(cpricing.InternetNetworkEgress, 64)
	if err != nil {
		return nil, err
	}

	return &models.Network{
		ZoneNetworkEgressCost:     znec,
		RegionNetworkEgressCost:   rnec,
		InternetNetworkEgressCost: inec,
	}, nil
}

func (yc *Yandex) LoadBalancerPricing() (*models.LoadBalancer, error) {
	cpricing, err := yc.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	lbPricing, err := strconv.ParseFloat(cpricing.DefaultLBPrice, 64)
	if err != nil {
		return nil, err
	}
	return &models.LoadBalancer{
		Cost: lbPricing,
	}, nil
}

func (yc *Yandex) AllNodePricing() (interface{}, error) {
	yc.DownloadPricingDataLock.RLock()
	defer yc.DownloadPricingDataLock.RUnlock()
	return yc.Pricing, nil
}

func (yc *Yandex) loadBillingDataFromCsv(ctx context.Context, sdk *ycsdk.SDK) error {
	header, err := csvutil.Header(sku{}, "csv")
	if err != nil {
		return err
	}
	fieldsPerRecord := len(header)
	var csvr io.Reader
	var csverr error
	if strings.HasPrefix(yc.CSVLocation, "s3://") {
		region := env.GetCSVRegion()
		conf := aws.NewConfig().WithRegion(region).WithCredentialsChainVerboseErrors(true)
		endpoint := env.GetCSVEndpoint()
		if endpoint != "" {
			conf = conf.WithEndpoint(endpoint)
		}
		s, err := session.NewSession(conf)
		if err != nil {
			return err
		}
		s3Client := s3.New(s)
		bucketAndKey := strings.Split(strings.TrimPrefix(yc.CSVLocation, "s3://"), "/")
		if len(bucketAndKey) == 2 {
			out, err := s3Client.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucketAndKey[0]),
				Key:    aws.String(bucketAndKey[1]),
			})
			csverr = err
			csvr = out.Body
		} else {
			return fmt.Errorf("invalid s3 URI: %s", yc.CSVLocation)
		}
	} else {
		csvr, csverr = os.Open(yc.CSVLocation)
	}
	if csverr != nil {
		log.Infof("Error reading csv at %s: %s", yc.CSVLocation, csverr)
		return nil
	}

	csvReader := csv.NewReader(csvr)
	csvReader.Comma = ','
	csvReader.FieldsPerRecord = fieldsPerRecord

	dec, err := csvutil.NewDecoder(csvReader, header...)
	if err != nil {
		return err
	}

	for {
		p := sku{}
		err := dec.Decode(&p)
		csvParseErr, isCsvParseErr := err.(*csv.ParseError)
		if strings.ToLower(p.SKUId) == "skuid" {
			continue
		}
		if err == io.EOF {
			break
		} else if err == csvutil.ErrFieldCount || (isCsvParseErr && csvParseErr.Err == csv.ErrFieldCount) {
			rec := dec.Record()
			if len(rec) != 1 {
				log.Infof("Expected %d price info fields but received %d: %s", fieldsPerRecord, len(rec), rec)
				continue
			}
			if strings.Index(rec[0], "#") == 0 {
				continue
			} else {
				log.Infof("skipping non-CSV line: %s", rec)
				continue
			}
		} else if err != nil {
			log.Infof("Error during spot info decode: %+v", err)
			continue
		}
		log.Infof("Found sku info %+v", p)
		cost, err := yc.GetCostBySKUId(ctx, sdk, p.SKUId)
		if err != nil {
			log.Infof("Error while getting sku value:%s", p.SKUId)
			cost = "0"
		}
		if _, ok := yc.Pricing[p.InstanceID]; !ok {
			if p.InstanceType == "PV" {
				yc.Pricing[p.InstanceID] = &YandexPricing{
					PV: &models.PV{Cost: cost,
						Class: p.InstanceID},
				}
			} else {
				yc.Pricing[p.InstanceID] = &YandexPricing{
					Node: &models.Node{},
				}
			}
		}
		if p.InstanceType == "VCPU" {
			yc.Pricing[p.InstanceID].Node.VCPUCost = cost
		} else if p.InstanceType == "RAM" {
			yc.Pricing[p.InstanceID].Node.RAMCost = cost
		} else if p.InstanceType == "Storage" {
			yc.Pricing[p.InstanceID].Node.StorageCost = cost
		} else if p.InstanceType == "GPU" {
			yc.Pricing[p.InstanceID].Node.GPUCost = cost
		}
	}

	return nil
}

func checkArch(nG *k8s.NodeGroup, sku *billing.Sku) bool {
	if slices.Contains(cpuPlatforms, nG.NodeTemplate.PlatformId) && (sku.ServiceId != COMPUTE_CLOUD_SERVICE_ID) {
		return false
	}
	if slices.Contains(gpuPlatforms, nG.NodeTemplate.PlatformId) && (sku.ServiceId != COMPUTE_CLOUD_GPU_SERVICE_ID) {
		return false
	}

	nGPlatform := platforms[nG.NodeTemplate.PlatformId]
	skuPlatform := strings.Split(sku.Name, ".")[0]
	return strings.EqualFold(nGPlatform, skuPlatform) || strings.EqualFold(fmt.Sprintf("%s vGPU", nGPlatform), skuPlatform)
}

func checkGPU(nG *k8s.NodeGroup, sku *billing.Sku) bool {
	return nG.NodeTemplate.ResourcesSpec.Gpus > 0 && strings.Contains(sku.Name, "GPU")
}

func checkCPU(nG *k8s.NodeGroup, sku *billing.Sku) bool {
	coreFraction := nG.NodeTemplate.ResourcesSpec.CoreFraction
	coreFractionMatched := false

	switch coreFraction {
	case 5:
		coreFractionMatched = strings.Contains(sku.Name, "5% vCPU")
	case 20:
		coreFractionMatched = strings.Contains(sku.Name, "20% vCPU")
	case 50:
		coreFractionMatched = strings.Contains(sku.Name, "50% vCPU")
	case 100:
		coreFractionMatched = strings.Contains(sku.Name, "100% vCPU")
	}
	return coreFractionMatched && !strings.Contains(sku.Name, "committed")
}

func checkPreemptible(nG *k8s.NodeGroup, sku *billing.Sku) bool {
	nGPreemptible := nG.NodeTemplate.SchedulingPolicy.Preemptible
	skuPreemptible := strings.Contains(sku.Name, "preemptible")
	return nGPreemptible == skuPreemptible
}

func checkRAM(sku *billing.Sku) bool {
	return strings.Contains(sku.Name, "RAM") && !strings.Contains(sku.Name, "committed")
}

func checkStorage(nG *k8s.NodeGroup, sku *billing.Sku) bool {
	if nG.NodeTemplate.BootDiskSpec.DiskTypeId == NETWORK_SSD_NONREPLICATED && strings.Contains(sku.Name, "Non-replicated fast network storage (SSD)") {
		return true
	} else if nG.NodeTemplate.BootDiskSpec.DiskTypeId == NETWORK_SSD && strings.Contains(sku.Name, "Fast network storage (SSD)") {
		return true
	} else if nG.NodeTemplate.BootDiskSpec.DiskTypeId == NETWORK_SSD_IO_M3 && strings.Contains(sku.Name, "Ultra fast network storage") {
		return true
	} else if strings.Contains(nG.NodeTemplate.BootDiskSpec.DiskTypeId, "edicated") && strings.Contains(sku.Name, "edicated") {
		return true
	} else if nG.NodeTemplate.BootDiskSpec.DiskTypeId == NETWORK_HDD && strings.Contains(sku.Name, "Standard network storage (HDD)") {
		return true
	} else if strings.Contains(sku.Name, "Standard file system (HDD)") && strings.Contains(nG.NodeTemplate.BootDiskSpec.DiskTypeId, "hdd") {
		return true
	} else if strings.Contains(sku.Name, "Fast file system (SSD)") && strings.Contains(nG.NodeTemplate.BootDiskSpec.DiskTypeId, "ssd") {
		return true
	}
	return false
}

func (yc *Yandex) GetDefaultNGBillingData(ctx context.Context, sdk *ycsdk.SDK, nG *k8s.NodeGroup, skus []*billing.Sku) (*YandexPricing, error) {
	pricing := &YandexPricing{
		Node: &models.Node{},
	}

	for _, sku := range skus {
		if checkPreemptible(nG, sku) && checkArch(nG, sku) {
			cost, err := yc.GetCostBySKUId(ctx, sdk, sku.Id)
			if err != nil {
				log.Infof("Error while getting sku value:%s", sku.Id)
				cost = "0"
			}
			skuCache.Set(sku.Id, sku, cache.DefaultExpiration)
			if checkCPU(nG, sku) {
				pricing.Node.VCPUCost = cost
				log.Infof("Found cpu sku - %s for node group: id - %s, name - %s", sku.Name, nG.Id, nG.Name)
				continue
			} else if checkGPU(nG, sku) {
				pricing.Node.GPUCost = cost
				log.Infof("Found gpu sku - %s for node group: id - %s, name - %s", sku.Name, nG.Id, nG.Name)
				continue
			} else if checkRAM(sku) {
				pricing.Node.RAMCost = cost
				log.Infof("Found ram sku - %s for node group: id - %s, name - %s", sku.Name, nG.Id, nG.Name)
				continue
			}
		}

		if pricing.Node.StorageCost == "" && checkStorage(nG, sku) {
			cost, err := yc.GetCostBySKUId(ctx, sdk, sku.Id)
			if err != nil {
				log.Infof("Error while getting sku value:%s", sku.Id)
				cost = "0"
			}
			pricing.Node.StorageCost = cost
			storagesCosts[nG.NodeTemplate.BootDiskSpec.DiskTypeId] = cost
			log.Infof("Found storage sku - %s for node group: id - %s, name - %s", sku.Name, nG.Id, nG.Name)
		}
	}

	return pricing, nil
}

func (yc *Yandex) addDefaultBillingData(ctx context.Context, sdk *ycsdk.SDK, nodeGroups []*k8s.NodeGroup) error {
	c, err := yc.Config.GetCustomPricingData()
	if err != nil {
		return err
	}
	skus := []*billing.Sku{}

	for _, serviceId := range []string{COMPUTE_CLOUD_SERVICE_ID, COMPUTE_CLOUD_GPU_SERVICE_ID} {
		response, err := sdk.Billing().Sku().List(ctx, &billing.ListSkusRequest{Currency: c.CurrencyCode, BillingAccountId: env.GetYandexBillingAccountID(), Filter: fmt.Sprintf("serviceId=\"%s\"", serviceId)})
		if err != nil {
			return err
		}
		skus = append(skus, response.Skus...)
	}

	for _, nG := range nodeGroups {
		pricing, ok := yc.Pricing[nG.Id]
		if !ok || pricing.Node == nil {
			pricing, err := yc.GetDefaultNGBillingData(ctx, sdk, nG, skus)
			if err != nil {
				log.Infof("can't set costs for node: %s, %s", nG.Id, err)
			}
			yc.Pricing[nG.Id] = pricing
		}
	}
	for storage, cost := range storagesCosts {
		pricing, ok := yc.Pricing[storage]
		if !ok || pricing.PV == nil {
			pricing := &YandexPricing{
				PV: &models.PV{Cost: cost},
			}
			yc.Pricing[storage] = pricing
		}
	}

	return nil
}

func (yc *Yandex) GetBillingData(ctx context.Context, sdk *ycsdk.SDK, nodeGroups []*k8s.NodeGroup) error {
	yc.Pricing = make(map[string]*YandexPricing)
	err := yc.loadBillingDataFromCsv(ctx, sdk)
	if err != nil {
		log.Warnf("Failed to load billing data from csv")
	}
	return yc.addDefaultBillingData(ctx, sdk, nodeGroups)
}

func (yc *Yandex) GetCostBySKUId(ctx context.Context, sdk *ycsdk.SDK, skuId string) (string, error) {
	var sku *billing.Sku
	if val, found := skuCache.Get(skuId); found {
		sku = val.(*billing.Sku)
	} else {
		c, err := yc.Config.GetCustomPricingData()
		if err != nil {
			return "0", err
		}

		sku, err = sdk.Billing().Sku().Get(ctx, &billing.GetSkuRequest{Currency: c.CurrencyCode, BillingAccountId: env.GetYandexBillingAccountID(), Id: skuId})
		if err != nil {
			return "0", err
		}
		skuCache.Set(sku.Id, sku, cache.DefaultExpiration)
	}

	if len(sku.PricingVersions) == 0 {
		log.Infof("Incorrect sku:%s", skuId)
		return "0", nil
	}
	pricingVersion := sku.PricingVersions[0]
	for _, item := range sku.PricingVersions {
		if item.EffectiveTime.Seconds > pricingVersion.EffectiveTime.Seconds {
			pricingVersion = item
		}
	}
	// TODO check it correctly
	if len(pricingVersion.PricingExpressions) == 0 || len(pricingVersion.PricingExpressions[0].Rates) == 0 {
		log.Infof("Incorrect sku:%s", skuId)
		return "0", nil
	}
	resRate := pricingVersion.PricingExpressions[0].Rates[0]
	for _, expression := range pricingVersion.PricingExpressions {
		for _, rate := range expression.Rates {
			if rate.UnitPrice > resRate.UnitPrice {
				resRate = rate
			}
		}

	}

	return resRate.UnitPrice, nil
}

// TODO refactoring
func (yc *Yandex) GetNodeGroups(ctx context.Context, sdk *ycsdk.SDK) ([]*k8s.NodeGroup, error) {
	var res []*k8s.NodeGroup
	var pageLoader func(string) error
	pageLoader = func(pageToken string) error {
		response, err := sdk.Kubernetes().Cluster().ListNodeGroups(ctx, &k8s.ListClusterNodeGroupsRequest{ClusterId: env.GetClusterProfile(), PageToken: pageToken})
		if err != nil {
			return err
		}
		res = append(res, response.NodeGroups...)
		if response.NextPageToken != "" {
			pageLoader(response.NextPageToken)
		}
		return nil
	}
	err := pageLoader("")
	return res, err
}

func (yc *Yandex) signedToken() string {
	claims := jwt.RegisteredClaims{
		Issuer:    yc.YCServiceKey.ServiceAccountID,
		ExpiresAt: jwt.NewNumericDate(time.Now().UTC().Add(1 * time.Hour)),
		IssuedAt:  jwt.NewNumericDate(time.Now().UTC()),
		NotBefore: jwt.NewNumericDate(time.Now().UTC()),
		Audience:  []string{"https://iam.api.cloud.yandex.net/iam/v1/tokens"},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodPS256, claims)
	token.Header["kid"] = yc.YCServiceKey.KeyID

	privateKey := yc.loadPrivateKey()
	signed, err := token.SignedString(privateKey)
	if err != nil {
		panic(err)
	}
	return signed
}

func (yc *Yandex) loadPrivateKey() *rsa.PrivateKey {
	rsaPrivateKey, err := jwt.ParseRSAPrivateKeyFromPEM([]byte(yc.YCServiceKey.PrivateKey))
	if err != nil {
		panic(err)
	}
	return rsaPrivateKey
}

func (yc *Yandex) getIAMToken() string {
	jot := yc.signedToken()
	resp, err := http.Post(
		"https://iam.api.cloud.yandex.net/iam/v1/tokens",
		"application/json",
		strings.NewReader(fmt.Sprintf(`{"jwt":"%s"}`, jot)),
	)
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		panic(fmt.Sprintf("%s: %s", resp.Status, body))
	}
	var data struct {
		IAMToken string `json:"iamToken"`
	}
	err = json.NewDecoder(resp.Body).Decode(&data)
	if err != nil {
		panic(err)
	}
	return data.IAMToken
}

func (yc *Yandex) DownloadPricingData() error {
	yc.DownloadPricingDataLock.Lock()
	defer yc.DownloadPricingDataLock.Unlock()

	_, err := yc.loadYCAuthSecret()

	token := yc.getIAMToken()
	if err != nil {
		log.Errorf("Error downloading yandex auth secret: %s", err.Error())
		return err
	}

	ctx := context.Background()
	// TODO get iam token by jwt https://cloud.yandex.ru/ru/docs/iam/operations/iam-token/create-for-sa#instruction_1 for service account and mount it as file somewhere
	// yc iam create-token - temporary
	sdk, err := ycsdk.Build(ctx, ycsdk.Config{
		Credentials: ycsdk.NewIAMTokenCredentials(token),
	})
	if err != nil {
		log.Fatalf("failed to connect to yc: %s", err.Error())
	}
	//nodeList := yc.Clientset.GetAllNodes()
	storageClasses := yc.Clientset.GetAllStorageClasses()
	yc.PVTypeByStorageClass = map[string]string{}

	for _, sClass := range storageClasses {
		classType, ok := sClass.Parameters["type"]
		if ok {
			yc.PVTypeByStorageClass[sClass.Name] = classType
		} else {
			yc.PVTypeByStorageClass[sClass.Name] = sClass.Name
		}
	}

	nodeGroups, err := yc.GetNodeGroups(ctx, sdk)
	if err != nil {
		log.Fatalf("failed to get node groups: %s", err.Error())
		return err
	}

	err = yc.GetBillingData(ctx, sdk, nodeGroups)
	if err != nil {
		log.Fatalf("failed to get billing data: %s", err.Error())
		return err
	}

	for _, nG := range nodeGroups {
		pricing, ok := yc.Pricing[nG.Id]
		if ok && pricing.Node != nil {
			pricing.Node.VCPU = fmt.Sprintf("%v", nG.NodeTemplate.ResourcesSpec.Cores)
			pricing.Node.Storage = fmt.Sprintf("%v", nG.NodeTemplate.BootDiskSpec.DiskSize)
			pricing.Node.RAM = fmt.Sprintf("%v", nG.NodeTemplate.ResourcesSpec.Memory)
			pricing.Node.UsageType = fmt.Sprintf("%v", nG.NodeTemplate.SchedulingPolicy.Preemptible)
		}
	}

	return nil
}
func (yc *Yandex) GetKey(labels map[string]string, n *clustercache.Node) models.Key {
	return &ycKey{
		Labels: labels,
	}
}
func (yc *Yandex) GetPVKey(pv *clustercache.PersistentVolume, parameters map[string]string, defaultRegion string) models.PVKey {
	return &ycPVKey{
		Labels:                 pv.Labels,
		StorageClassName:       pv.Spec.StorageClassName,
		StorageClassParameters: parameters,
	}
}
func (yc *Yandex) UpdateConfig(r io.Reader, updateType string) (*models.CustomPricing, error) {
	// TODO not implemented
	return nil, nil
}

// Attempts to load a GCP auth secret and copy the contents to the key file.
func (yc *Yandex) loadYCAuthSecret() (*ycServiceKey, error) {
	path := env.GetConfigPathWithDefault("/models/")

	keyPath := path + "key.json"
	keyExists, _ := fileutil.FileExists(keyPath)
	if keyExists {
		log.Info("YC Auth Key already exists, no need to load from secret")
	} else {
		exists, err := fileutil.FileExists(models.AuthSecretPath)
		if !exists || err != nil {
			errMessage := "Secret does not exist"
			if err != nil {
				errMessage = err.Error()
			}

			log.Warnf("Failed to load auth secret, or was not mounted: %s", errMessage)
			return nil, err
		}

		result, err := os.ReadFile(models.AuthSecretPath)
		if err != nil {
			log.Warnf("Failed to load auth secret, or was not mounted: %s", err.Error())
			return nil, err
		}

		err = os.WriteFile(keyPath, result, 0644)
		if err != nil {
			log.Warnf("Failed to copy auth secret to %s: %s", keyPath, err.Error())
		}
	}

	result, err := os.ReadFile(keyPath)
	if err != nil {
		log.Warnf("Failed to read file %s: %s", keyPath, err.Error())
		return nil, err
	}
	var ysk ycServiceKey
	err = json.Unmarshal(result, &ysk)
	if err != nil {
		log.Warnf("Failed to unmarshal file %s: %s", keyPath, err.Error())
		return nil, err
	}
	yc.YCServiceKey = &ysk
	return &ysk, nil
}

func (yc *Yandex) UpdateConfigFromConfigMap(a map[string]string) (*models.CustomPricing, error) {
	return yc.Config.UpdateFromMap(a)
}

func (yc *Yandex) GetConfig() (*models.CustomPricing, error) {
	c, err := yc.Config.GetCustomPricingData()
	if err != nil {
		return nil, err
	}
	if c.CurrencyCode == "" {
		c.CurrencyCode = "RUB"
	}

	return c, nil
}
func (yc *Yandex) GetManagementPlatform() (string, error) {
	return "", nil
}
func (yc *Yandex) GetLocalStorageQuery(time.Duration, time.Duration, bool, bool) string {
	return ""
}
func (yc *Yandex) ApplyReservedInstancePricing(map[string]*models.Node) {

}
func (yc *Yandex) ServiceAccountStatus() *models.ServiceAccountStatus {
	// TODO fix
	return &models.ServiceAccountStatus{}
}
func (yc *Yandex) PricingSourceStatus() map[string]*models.PricingSource {
	// TODO fix
	return map[string]*models.PricingSource{}
}
func (yc *Yandex) ClusterManagementPricing() (string, float64, error) {
	// TODO it depends on type: zonal or regional https://cloud.yandex.com/ru/docs/managed-kubernetes/pricing
	return "", 0, nil
}
func (yc *Yandex) CombinedDiscountForNode(string, bool, float64, float64) float64 {
	// TODO Calculate discount
	return 0.0
}
func (yc *Yandex) Regions() []string {
	return ycRegions
}
func (yc *Yandex) PricingSourceSummary() interface{} {
	return yc.Pricing
}
