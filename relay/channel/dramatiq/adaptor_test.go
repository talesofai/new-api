package dramatiq

import (
	"testing"

	"github.com/QuantumNous/new-api/dto"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/stretchr/testify/require"
)

func TestConvertImageRequestRejectsNGreaterThanOne(t *testing.T) {
	adaptor := &Adaptor{}
	n := uint(2)
	_, err := adaptor.ConvertImageRequest(nil, &relaycommon.RelayInfo{
		RelayMode: relayconstant.RelayModeImagesGenerations,
		ChannelMeta: &relaycommon.ChannelMeta{
			UpstreamModelName: "dramatiq-noobxl-t2i",
		},
	}, dto.ImageRequest{Model: "dramatiq-noobxl-t2i", Prompt: "cat", N: &n})
	require.ErrorContains(t, err, "only supports n=1")
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

func TestExtractCallbackResult(t *testing.T) {
	url, _, errMsg := extractCallbackResult(map[string]any{"img_url": "https://example.com/a.png", "error_msg": ""})
	require.Equal(t, "https://example.com/a.png", url)
	require.Empty(t, errMsg)
}
