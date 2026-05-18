package dramatiq

import (
	"encoding/json"
	"testing"

	"github.com/QuantumNous/new-api/dto"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/stretchr/testify/require"
)

func TestConvertImageRequestRejectsNGreaterThanOne(t *testing.T) {
	adaptor := &Adaptor{}
	n := uint(2)
	_, err := adaptor.ConvertImageRequest(nil, dramatiqRelayInfo(
		relayconstant.RelayModeImagesGenerations,
		"dramatiq-noobxl-t2i",
	), dto.ImageRequest{Model: "dramatiq-noobxl-t2i", Prompt: "cat", N: &n})
	require.ErrorContains(t, err, "only supports n=1")
}

func TestConvertImageRequestRejectsB64JSON(t *testing.T) {
	adaptor := &Adaptor{}
	_, err := adaptor.ConvertImageRequest(nil, dramatiqRelayInfo(
		relayconstant.RelayModeImagesGenerations,
		"dramatiq-noobxl-t2i",
	), dto.ImageRequest{Model: "dramatiq-noobxl-t2i", Prompt: "cat", ResponseFormat: "b64_json"})
	require.ErrorContains(t, err, "response_format=url")
}

func TestBuildT2IPayload(t *testing.T) {
	payload, err := buildAPIPayload(nil, modelConfig{
		Adopt:    adoptT2I,
		TaskName: defaultTaskName,
		Workflow: "3_noobxl/t2i_base_oc_ref_v1.json",
	}, dto.ImageRequest{Prompt: "cat"}, 1024, 1024)
	require.NoError(t, err)
	require.Equal(t, "cat", payload["positive_prompt"])
	require.Equal(t, "3_noobxl/t2i_base_oc_ref_v1.json", payload["workflow_name"])
	require.Equal(t, 1024, payload["width"])
}

func TestBuildI2IPayloadRequiresImage(t *testing.T) {
	_, err := buildAPIPayload(nil, modelConfig{Adopt: adoptI2I, Workflow: "3_noobxl/i2i_tile_v1.json", TaskName: defaultTaskName}, dto.ImageRequest{Prompt: "cat"}, 1024, 1024)
	require.ErrorContains(t, err, "image_url is required")
}

func TestBuildI2IPayloadRejectsImageURLs(t *testing.T) {
	_, err := buildAPIPayload(nil, modelConfig{Adopt: adoptI2I, Workflow: "3_noobxl/i2i_tile_v1.json", TaskName: defaultTaskName}, dto.ImageRequest{
		Prompt: "cat",
		Extra: map[string]json.RawMessage{
			"image_urls": json.RawMessage(`["https://example.com/a.png"]`),
		},
	}, 1024, 1024)
	require.ErrorContains(t, err, "single image_url")
}

func TestResolveModelConfigFromChannelSetting(t *testing.T) {
	cfg, err := resolveModelConfig("dramatiq-noobxl-t2i", dramatiqChannelSetting())
	require.NoError(t, err)
	require.Equal(t, "redis://localhost:6379/1", cfg.BrokerURL)
	require.Equal(t, "dramatiq", cfg.Namespace)
	require.Equal(t, "d_noob_base", cfg.Queue)
	require.Equal(t, "3_noobxl/t2i_base_oc_ref_v1.json", cfg.Workflow)
}

func TestResolveModelConfigRequiresQueue(t *testing.T) {
	setting := dramatiqChannelSetting()
	model := setting.DramatiqModels["dramatiq-noobxl-t2i"]
	model.Queue = ""
	setting.DramatiqModels["dramatiq-noobxl-t2i"] = model
	_, err := resolveModelConfig("dramatiq-noobxl-t2i", setting)
	require.ErrorContains(t, err, "missing queue")
}

func TestResolveModelConfigRequiresBrokerURL(t *testing.T) {
	setting := dramatiqChannelSetting()
	setting.DramatiqBrokerURL = ""
	_, err := resolveModelConfig("dramatiq-noobxl-t2i", setting)
	require.ErrorContains(t, err, "dramatiq_broker_url is required")
}

func TestExtractCallbackResult(t *testing.T) {
	url, errMsg := extractCallbackResult(map[string]any{"img_url": "https://example.com/a.png", "error_msg": ""})
	require.Equal(t, "https://example.com/a.png", url)
	require.Empty(t, errMsg)
}

func TestExtractCallbackResultFromArray(t *testing.T) {
	url, errMsg := extractCallbackResult([]any{map[string]any{"img_url": "https://example.com/a.png"}})
	require.Equal(t, "https://example.com/a.png", url)
	require.Empty(t, errMsg)
}

func TestExtractCallbackResultFailure(t *testing.T) {
	url, errMsg := extractCallbackResult(map[string]any{"error_msg": "workflow failed"})
	require.Empty(t, url)
	require.Equal(t, "workflow failed", errMsg)
}

func TestExtractCallbackResultRejectsStringShape(t *testing.T) {
	url, errMsg := extractCallbackResult("https://example.com/a.png")
	require.Empty(t, url)
	require.Empty(t, errMsg)
}

func dramatiqRelayInfo(relayMode int, upstreamModelName string) *relaycommon.RelayInfo {
	return &relaycommon.RelayInfo{
		RelayMode: relayMode,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: upstreamModelName,
			ChannelSetting:    dramatiqChannelSetting(),
		},
	}
}

func dramatiqChannelSetting() dto.ChannelSettings {
	return dto.ChannelSettings{
		DramatiqBrokerURL: "redis://localhost:6379/1",
		DramatiqModels: map[string]dto.DramatiqModelSetting{
			"dramatiq-noobxl-t2i": {
				Adopt:    adoptT2I,
				Queue:    "d_noob_base",
				Workflow: "3_noobxl/t2i_base_oc_ref_v1.json",
			},
		},
	}
}
