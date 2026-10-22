package libXray

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/xtls/libxray/failover"
	"github.com/xtls/libxray/geo"
	"github.com/xtls/libxray/nodep"
	"github.com/xtls/libxray/probe"
	"github.com/xtls/libxray/share"
	"github.com/xtls/libxray/xray"
)

type invokeResponse struct {
	Success bool   `json:"success"`
	Data    any    `json:"data"`
	Err     string `json:"error"`
}

const (
	maxInvokeJSONSizeMiB = 16
	maxInvokeJSONBytes   = maxInvokeJSONSizeMiB * 1024 * 1024
)

func Invoke(requestJSON string) string {
	if len(requestJSON) > maxInvokeJSONBytes {
		return encodeInvokeResponse(nil, errors.New(invokeJSONSizeLimitMessage("request")))
	}
	var request LibXrayInvokeRequest
	if err := json.Unmarshal([]byte(requestJSON), &request); err != nil {
		return encodeInvokeResponse(nil, err)
	}
	if err := validateAPIVersion(request.APIVersion); err != nil {
		return encodeInvokeResponse(nil, err)
	}

	switch request.Method {
	case LibXrayMethodGetFreePorts:
		return invokeGetFreePorts(request.Payload)
	case LibXrayMethodDecodeSubscriptionText:
		return invokeDecodeSubscriptionText(request.Payload)
	case LibXrayMethodConvertShareLinksToXrayJson:
		return invokeConvertShareLinksToXrayJson(request.Payload)
	case LibXrayMethodConvertXrayJsonToShareLinks:
		return invokeConvertXrayJsonToShareLinks(request.Payload)
	case LibXrayMethodGenerateAgeKeyPair:
		return invokeGenerateAgeKeyPair(request.Payload)
	case LibXrayMethodCountGeoData:
		return invokeCountGeoData(request.Payload)
	case LibXrayMethodPingBatch:
		return invokePingBatch(request.Payload)
	case LibXrayMethodTestXray:
		return invokeTestXray(request.Payload)
	case LibXrayMethodRunXray:
		return invokeRunXray(request.Payload)
	case LibXrayMethodStopXray:
		return invokeStopXray()
	case LibXrayMethodGetFailoverState:
		return encodeInvokeResponse(failover.CurrentState(), nil)
	case LibXrayMethodSetFailoverOrder:
		return invokeSetFailoverOrder(request.Payload)
	case LibXrayMethodStartProbe:
		return invokeStartProbe(request.Payload)
	case LibXrayMethodProbeResults:
		return encodeInvokeResponse(probe.Results(), nil)
	case LibXrayMethodStopProbe:
		probe.StopSession()
		return encodeInvokeNoDataResponse(nil)
	case LibXrayMethodCloseProbeHost:
		probe.CloseHost()
		return encodeInvokeNoDataResponse(nil)
	case LibXrayMethodXrayVersion:
		return encodeInvokeResponse(&XrayVersionResponse{Version: xray.XrayVersion()}, nil)
	case LibXrayMethodGetXrayState:
		return encodeInvokeResponse(&GetXrayStateResponse{Running: xray.GetXrayState()}, nil)
	default:
		return encodeInvokeResponse(nil, errors.New("unknown method"))
	}
}
func validateAPIVersion(version int) error {
	if version == LibXrayAPIVersion {
		return nil
	}
	return errors.New("unsupported apiVersion")
}

func decodePayload[T any](payload json.RawMessage) (T, error) {
	var request T
	if len(payload) == 0 {
		return request, nil
	}
	err := json.Unmarshal(payload, &request)
	return request, err
}

func encodeInvokeResponse(data any, err error) string {
	response := invokeResponse{Data: data}
	if err != nil {
		response.Success = false
		response.Err = err.Error()
	} else {
		response.Success = true
	}
	raw, err := json.Marshal(&response)
	if err != nil {
		return `{"success":false,"data":null,"error":"failed to encode response"}`
	}
	if len(raw) > maxInvokeJSONBytes {
		return encodeInvokeFailure(invokeJSONSizeLimitMessage("response"))
	}
	return string(raw)
}

func invokeJSONSizeLimitMessage(kind string) string {
	return fmt.Sprintf(
		"invoke %s exceeds the %d MiB size limit",
		kind,
		maxInvokeJSONSizeMiB,
	)
}

func encodeInvokeFailure(message string) string {
	raw, err := json.Marshal(&invokeResponse{Err: message})
	if err != nil {
		return `{"success":false,"data":null,"error":"failed to encode response"}`
	}
	return string(raw)
}

func encodeInvokeNoDataResponse(err error) string {
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	return encodeInvokeResponse(struct{}{}, nil)
}

func invokeGetFreePorts(payload json.RawMessage) string {
	request, err := decodePayload[GetFreePortsRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	ports, err := nodep.GetFreePorts(request.Count, request.ExcludePorts)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	return encodeInvokeResponse(&GetFreePortsResponse{Ports: ports}, nil)
}

func invokeDecodeSubscriptionText(payload json.RawMessage) string {
	request, err := decodePayload[DecodeSubscriptionTextRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	secretKey := ""
	if request.Age != nil {
		secretKey = request.Age.SecretKey
	}
	text, err := share.DecodeSubscriptionText(request.Text, secretKey)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	return encodeInvokeResponse(&DecodeSubscriptionTextResponse{Text: text}, nil)
}

func invokeConvertShareLinksToXrayJson(payload json.RawMessage) string {
	request, err := decodePayload[ConvertShareLinksToXrayJsonRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	secretKey := ""
	if request.Age != nil {
		secretKey = request.Age.SecretKey
	}
	result, err := share.ConvertShareLinksToXrayJson(request.Text, secretKey)
	return encodeInvokeResponse(result, err)
}

func invokeGenerateAgeKeyPair(payload json.RawMessage) string {
	request, err := decodePayload[GenerateAgeKeyPairRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	pair, err := share.GenerateAgeKeyPair(share.AgeKeyType(request.KeyType))
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	return encodeInvokeResponse(&GenerateAgeKeyPairResponse{
		SecretKey: pair.SecretKey,
		PublicKey: pair.PublicKey,
	}, nil)
}

func invokeConvertXrayJsonToShareLinks(payload json.RawMessage) string {
	request, err := decodePayload[ConvertXrayJsonToShareLinksRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	links, err := share.ConvertXrayJsonToShareLinks([]byte(request.XrayJson))
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	return encodeInvokeResponse(&ConvertXrayJsonToShareLinksResponse{Links: links}, nil)
}

func invokeCountGeoData(payload json.RawMessage) string {
	request, err := decodePayload[CountGeoDataRequest](payload)
	if err != nil {
		return encodeInvokeNoDataResponse(err)
	}
	if request.DatDir == "" {
		return encodeInvokeNoDataResponse(errors.New("missing datDir"))
	}
	err = geo.CountGeoData(request.DatDir, request.Name, request.GeoType)
	return encodeInvokeNoDataResponse(err)
}

func invokePingBatch(payload json.RawMessage) string {
	request, err := decodePayload[PingBatchRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}

	configs := make([]xray.PingBatchItem, len(request.Configs))
	for i, config := range request.Configs {
		configs[i] = xray.PingBatchItem{
			XrayJSON:    config.XrayJson,
			OutboundTag: config.OutboundTag,
		}
	}

	results, err := xray.PingBatch(
		configs,
		request.Timeout,
		request.URL,
	)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}

	responseResults := make([]PingBatchItemResponse, len(results))
	for i, result := range results {
		responseResults[i] = PingBatchItemResponse{
			Success: result.Success,
			Delay:   result.Delay,
			Error:   result.Error,
		}
	}
	return encodeInvokeResponse(&PingBatchResponse{Results: responseResults}, nil)
}

func invokeTestXray(payload json.RawMessage) string {
	request, err := decodePayload[TestXrayRequest](payload)
	if err != nil {
		return encodeInvokeNoDataResponse(err)
	}
	err = xray.TestXray(request.XrayJson)
	return encodeInvokeNoDataResponse(err)
}

// lifecycle serializes runXray and stopXray end to end, so the failover
// controller always matches the core: a run and a stop can't interleave
// between RunXray and failover.Start. Core callbacks, the probe and
// getFailoverState never take it; the lock order is lifecycle → probe →
// the core's own lock.
var lifecycle sync.Mutex

func invokeRunXray(payload json.RawMessage) string {
	request, err := decodePayload[RunXrayRequest](payload)
	if err != nil {
		return encodeInvokeNoDataResponse(err)
	}
	lifecycle.Lock()
	defer lifecycle.Unlock()
	// A sweep's dials must not race the new core's construction (core.New
	// repoints the process-global dialer), and its readings across a
	// connect are discarded anyway.
	probe.StopSession()
	// A refusal — xray.ErrAlreadyRunning for a duplicate — leaves the
	// running tunnel and its controller as they are.
	if err = xray.RunXray(request.XrayJson); err != nil {
		return encodeInvokeNoDataResponse(err)
	}
	var settings failover.Settings
	if len(request.Failover) == 0 || json.Unmarshal(request.Failover, &settings) != nil {
		failover.Stop()
		return encodeInvokeNoDataResponse(nil)
	}
	// A controller that cannot start leaves the tunnel up on the balancer's
	// own strategy; getFailoverState reports why.
	_ = failover.Start(xray.Instance(), settings)
	return encodeInvokeNoDataResponse(nil)
}

func invokeStopXray() string {
	lifecycle.Lock()
	defer lifecycle.Unlock()
	// The controller probes through the instance: stop it first. A latency
	// sweep's readings across a disconnect are discarded anyway.
	failover.Stop()
	probe.StopSession()
	err := xray.StopXray()
	// Closing the tunnel's core leaves the process-global dialer on a dead
	// instance; the probe host is the live one now.
	probe.PointDialerAtHost()
	return encodeInvokeNoDataResponse(err)
}

func invokeSetFailoverOrder(payload json.RawMessage) string {
	request, err := decodePayload[SetFailoverOrderRequest](payload)
	if err != nil {
		return encodeInvokeNoDataResponse(err)
	}
	return encodeInvokeNoDataResponse(failover.SetOrder(request.Order))
}

func invokeStartProbe(payload json.RawMessage) string {
	request, err := decodePayload[probe.StartRequest](payload)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	response, err := probe.Start(request)
	if err != nil {
		return encodeInvokeResponse(nil, err)
	}
	return encodeInvokeResponse(&response, nil)
}
