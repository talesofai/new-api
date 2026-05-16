package dramatiq

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	relayconstant "github.com/QuantumNous/new-api/relay/constant"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/types"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

const (
	adoptT2I             = "t2i"
	adoptI2I             = "i2i"
	adoptSingleImageTool = "single_image_tool"

	defaultActor          = "comfyui"
	defaultQueue          = "cpu"
	defaultTaskName       = "make_image_with_comfy_common"
	defaultTimeoutSeconds = 60
	callbackPath          = "/v1/dramatiq/callback/image"
	resultKeyPrefix       = "dramatiq:image:result:"
)

type Adaptor struct {
	ChannelType int
}

func (a *Adaptor) Init(info *relaycommon.RelayInfo) {
	a.ChannelType = info.ChannelType
}

func (a *Adaptor) GetRequestURL(info *relaycommon.RelayInfo) (string, error) { return "", nil }
func (a *Adaptor) SetupRequestHeader(c *gin.Context, req *http.Header, info *relaycommon.RelayInfo) error {
	channel.SetupApiRequestHeader(info, c, req)
	return nil
}

func (a *Adaptor) ConvertOpenAIRequest(*gin.Context, *relaycommon.RelayInfo, *dto.GeneralOpenAIRequest) (any, error) {
	return nil, errors.New("dramatiq does not support chat requests")
}
func (a *Adaptor) ConvertClaudeRequest(*gin.Context, *relaycommon.RelayInfo, *dto.ClaudeRequest) (any, error) {
	return nil, errors.New("dramatiq does not support claude requests")
}
func (a *Adaptor) ConvertGeminiRequest(*gin.Context, *relaycommon.RelayInfo, *dto.GeminiChatRequest) (any, error) {
	return nil, errors.New("dramatiq does not support gemini requests")
}
func (a *Adaptor) ConvertEmbeddingRequest(*gin.Context, *relaycommon.RelayInfo, dto.EmbeddingRequest) (any, error) {
	return nil, errors.New("dramatiq does not support embedding requests")
}
func (a *Adaptor) ConvertAudioRequest(*gin.Context, *relaycommon.RelayInfo, dto.AudioRequest) (io.Reader, error) {
	return nil, errors.New("dramatiq does not support audio requests")
}
func (a *Adaptor) ConvertRerankRequest(*gin.Context, int, dto.RerankRequest) (any, error) {
	return nil, errors.New("dramatiq does not support rerank requests")
}
func (a *Adaptor) ConvertOpenAIResponsesRequest(*gin.Context, *relaycommon.RelayInfo, dto.OpenAIResponsesRequest) (any, error) {
	return nil, errors.New("dramatiq does not support responses requests")
}

func (a *Adaptor) ConvertImageRequest(c *gin.Context, info *relaycommon.RelayInfo, request dto.ImageRequest) (any, error) {
	if request.N != nil && *request.N > 1 {
		return nil, fmt.Errorf("dramatiq provider only supports n=1")
	}
	if strings.EqualFold(request.ResponseFormat, "b64_json") {
		return nil, fmt.Errorf("dramatiq provider only supports response_format=url")
	}

	cfg, err := resolveModelConfig(info.UpstreamModelName)
	if err != nil {
		return nil, err
	}
	if info.RelayMode == relayconstant.RelayModeImagesGenerations && cfg.Adopt != adoptT2I {
		return nil, fmt.Errorf("model %s must be used with /v1/images/edits", info.OriginModelName)
	}
	if info.RelayMode == relayconstant.RelayModeImagesEdits && cfg.Adopt == adoptT2I {
		return nil, fmt.Errorf("model %s must be used with /v1/images/generations", info.OriginModelName)
	}

	width, height, err := parseSize(request.Size)
	if err != nil {
		return nil, err
	}
	taskID := uuid.New().String()
	apiPayload, err := buildAPIPayload(c, cfg, request, width, height)
	if err != nil {
		return nil, err
	}

	timeoutSeconds := cfg.TimeoutSecs
	if timeoutSeconds <= 0 {
		timeoutSeconds = defaultTimeoutSeconds
	}
	backendURL := strings.TrimRight(service.GetCallbackAddress(), "/")
	params := map[string]any{
		"task_name":             cfg.TaskName,
		"extra_jobs":            "",
		"need_image_moderation": false,
		"no_callback":           false,
		"backend_url":           backendURL,
		"callback_path":         callbackPath,
		"timeout":               timeoutSeconds,
		"api_payload":           apiPayload,
		"worker":                "autodl",
		"ap_cost":               0,
		"ap_cost_original":      0,
		"ap_discount_percent":   nil,
	}
	return &convertedRequest{TaskID: taskID, Params: params, TimeoutSeconds: timeoutSeconds}, nil
}

func (a *Adaptor) DoRequest(c *gin.Context, info *relaycommon.RelayInfo, requestBody io.Reader) (any, error) {
	var req convertedRequest
	if err := common.DecodeJson(requestBody, &req); err != nil {
		return nil, err
	}
	cfg, err := resolveModelConfig(info.UpstreamModelName)
	if err != nil {
		return nil, err
	}
	if err := enqueueTask(c.Request.Context(), cfg, req.TaskID, req.Params); err != nil {
		return nil, err
	}

	body, _ := common.Marshal(req)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(body)),
	}, nil
}

func (a *Adaptor) DoResponse(c *gin.Context, resp *http.Response, info *relaycommon.RelayInfo) (any, *types.NewAPIError) {
	defer service.CloseResponseBodyGracefully(resp)
	var req convertedRequest
	if err := common.DecodeJson(resp.Body, &req); err != nil {
		return nil, types.NewOpenAIError(err, types.ErrorCodeBadResponseBody, http.StatusInternalServerError)
	}
	result, err := waitResult(c.Request.Context(), req.TaskID, time.Duration(req.TimeoutSeconds)*time.Second)
	if err != nil {
		return nil, types.NewOpenAIError(err, types.ErrorCodeDoRequestFailed, http.StatusGatewayTimeout)
	}
	if !isSuccessStatus(result.Status) {
		msg := result.Error
		if msg == "" {
			msg = "dramatiq image task failed"
		}
		return nil, types.NewOpenAIError(errors.New(msg), types.ErrorCodeBadResponse, http.StatusBadGateway)
	}
	if result.URL == "" && result.B64JSON == "" {
		return nil, types.NewOpenAIError(errors.New("dramatiq image task returned empty result"), types.ErrorCodeEmptyResponse, http.StatusBadGateway)
	}

	image := dto.ImageData{Url: result.URL, B64Json: result.B64JSON}
	imageResp := dto.ImageResponse{
		Created: common.GetTimestamp(),
		Data:    []dto.ImageData{image},
	}
	body, err := common.Marshal(imageResp)
	if err != nil {
		return nil, types.NewOpenAIError(err, types.ErrorCodeJsonMarshalFailed, http.StatusInternalServerError)
	}
	service.IOCopyBytesGracefully(c, resp, body)
	return &dto.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 1}, nil
}

func (a *Adaptor) GetModelList() []string { return ModelList }
func (a *Adaptor) GetChannelName() string { return ChannelName }

func resolveModelConfig(model string) (modelConfig, error) {
	configs := defaultModelConfigs()
	cfg, ok := configs[model]
	if !ok {
		return modelConfig{}, fmt.Errorf("unsupported dramatiq model: %s", model)
	}
	if cfg.Actor == "" {
		cfg.Actor = defaultActor
	}
	if cfg.Queue == "" {
		cfg.Queue = defaultQueue
	}
	if cfg.TaskName == "" {
		cfg.TaskName = defaultTaskName
	}
	if cfg.TimeoutSecs <= 0 {
		cfg.TimeoutSecs = defaultTimeoutSeconds
	}
	return cfg, nil
}

func defaultModelConfigs() map[string]modelConfig {
	return map[string]modelConfig{
		"dramatiq-noobxl-t2i":          {Adopt: adoptT2I, Workflow: "3_noobxl/t2i_base_oc_ref_v1.json"},
		"dramatiq-lumina-t2i":          {Adopt: adoptT2I, Workflow: "5_lumina/gpu_t2i_base.json"},
		"dramatiq-noobxl-i2i-tile":     {Adopt: adoptI2I, Workflow: "3_noobxl/i2i_tile_v1.json"},
		"dramatiq-noobxl-i2i-ipa":      {Adopt: adoptI2I, Workflow: "3_noobxl/i2i_ipa_v1.json"},
		"dramatiq-noobxl-i2i-openpose": {Adopt: adoptI2I, Workflow: "3_noobxl/i2i_openpose_v1.json"},
		"dramatiq-remove-bg":           {Adopt: adoptSingleImageTool, Workflow: "templates/i2i_remove_bg_v2_BiRefNet.json"},
		"dramatiq-lineart":             {Adopt: adoptSingleImageTool, Workflow: "templates/i2i_lineart_v1.json"},
	}
}

func buildAPIPayload(c *gin.Context, cfg modelConfig, request dto.ImageRequest, width, height int) (map[string]any, error) {
	extra, err := rawExtra(request)
	if err != nil {
		return nil, err
	}
	seed := int64(-1)
	if v, ok := extra["seed"]; ok {
		seed, _ = toInt64(v)
	}
	negative := toString(extra["negative_prompt"])
	prompt := request.Prompt
	if prompt == "" {
		prompt = toString(extra["prompt"])
	}

	payload := map[string]any{
		"task_name":       cfg.TaskName,
		"workflow_name":   cfg.Workflow,
		"positive_prompt": prompt,
		"negative_prompt": negative,
		"width":           width,
		"height":          height,
		"seed":            seed,
	}
	for k, v := range extra {
		if !reservedExtra(k) {
			payload[k] = v
		}
	}

	switch cfg.Adopt {
	case adoptT2I:
		return payload, nil
	case adoptI2I:
		imageURL := firstImageURL(c, request, extra)
		if imageURL == "" {
			return nil, errors.New("image_url is required for dramatiq i2i models")
		}
		payload["controlnetunit_tile_image_ref"] = imageURL
		payload["controlnetunit_tile_weight"] = floatOrDefault(extra["controlnet_weight"], 0.8)
		if faceRef := toString(extra["ipadapter_face_image_ref"]); faceRef != "" {
			payload["ipadapter_face_image_ref"] = faceRef
			payload["ipadapter_face_weight"] = floatOrDefault(extra["ipadapter_face_weight"], 0.6)
		}
		return payload, nil
	case adoptSingleImageTool:
		imageURL := firstImageURL(c, request, extra)
		if imageURL == "" {
			return nil, errors.New("image_url is required for dramatiq image tool models")
		}
		return map[string]any{
			"task_name":     cfg.TaskName,
			"workflow_name": cfg.Workflow,
			"image_url":     imageURL,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported dramatiq adopt: %s", cfg.Adopt)
	}
}

func rawExtra(request dto.ImageRequest) (map[string]any, error) {
	out := map[string]any{}
	for k, raw := range request.Extra {
		var v any
		if err := common.Unmarshal(raw, &v); err != nil {
			return nil, fmt.Errorf("invalid extra field %s: %w", k, err)
		}
		out[k] = v
	}
	if len(request.ExtraFields) > 0 {
		var m map[string]any
		if err := common.Unmarshal(request.ExtraFields, &m); err != nil {
			return nil, fmt.Errorf("invalid extra_fields: %w", err)
		}
		for k, v := range m {
			out[k] = v
		}
	}
	return out, nil
}

func reservedExtra(k string) bool {
	switch k {
	case "prompt", "negative_prompt", "seed", "image", "images", "image_url", "image_urls", "controlnet_weight", "ipadapter_face_image_ref", "ipadapter_face_weight":
		return true
	default:
		return false
	}
}

func parseSize(size string) (int, int, error) {
	if size == "" || size == "auto" {
		return 1024, 1024, nil
	}
	parts := strings.Split(size, "x")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid size: %s", size)
	}
	w, err := strconv.Atoi(parts[0])
	if err != nil || w <= 0 {
		return 0, 0, fmt.Errorf("invalid size width: %s", size)
	}
	h, err := strconv.Atoi(parts[1])
	if err != nil || h <= 0 {
		return 0, 0, fmt.Errorf("invalid size height: %s", size)
	}
	return w, h, nil
}

func firstImageURL(c *gin.Context, request dto.ImageRequest, extra map[string]any) string {
	for _, key := range []string{"image_url", "image"} {
		if s := toString(extra[key]); s != "" {
			return s
		}
	}
	if s := firstString(extra["image_urls"]); s != "" {
		return s
	}
	if s := firstString(extra["images"]); s != "" {
		return s
	}
	if s := firstRawString(request.Image); s != "" {
		return s
	}
	if s := firstRawString(request.Images); s != "" {
		return s
	}
	if c != nil && c.Request != nil && c.Request.MultipartForm != nil {
		if values := c.Request.MultipartForm.Value["image"]; len(values) > 0 {
			return strings.TrimSpace(values[0])
		}
	}
	return ""
}

func firstRawString(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := common.Unmarshal(raw, &s); err == nil {
		return strings.TrimSpace(s)
	}
	var arr []string
	if err := common.Unmarshal(raw, &arr); err == nil && len(arr) > 0 {
		return strings.TrimSpace(arr[0])
	}
	return ""
}

func firstString(v any) string {
	if s := toString(v); s != "" {
		return s
	}
	if arr, ok := v.([]any); ok && len(arr) > 0 {
		return toString(arr[0])
	}
	return ""
}

func toString(v any) string {
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func toInt64(v any) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int:
		return int64(t), true
	case int64:
		return t, true
	case string:
		i, err := strconv.ParseInt(t, 10, 64)
		return i, err == nil
	default:
		return 0, false
	}
}

func floatOrDefault(v any, fallback float64) float64 {
	if f, ok := v.(float64); ok {
		return f
	}
	return fallback
}

func enqueueTask(ctx context.Context, cfg modelConfig, taskID string, params map[string]any) error {
	if common.RDB == nil {
		return errors.New("redis is not enabled")
	}
	redisMessageID := uuid.New().String()
	msg := dramatiqMessage{
		QueueName: cfg.Queue,
		ActorName: cfg.Actor,
		Args:      []any{},
		Kwargs: map[string]any{
			"task_id": taskID,
			"params":  params,
		},
		Options: map[string]any{
			"trace_context":    map[string]any{},
			"redis_message_id": redisMessageID,
		},
		MessageID:        uuid.New().String(),
		MessageTimestamp: time.Now().UnixMilli(),
	}
	encoded, err := common.Marshal(msg)
	if err != nil {
		return err
	}
	pipe := common.RDB.TxPipeline()
	pipe.HSet(ctx, "dramatiq:"+cfg.Queue+".msgs", redisMessageID, encoded)
	pipe.RPush(ctx, "dramatiq:"+cfg.Queue, redisMessageID)
	_, err = pipe.Exec(ctx)
	return err
}

func waitResult(ctx context.Context, taskID string, timeout time.Duration) (*callbackResult, error) {
	deadline := time.Now().Add(timeout)
	key := resultKey(taskID)
	for {
		val, err := common.RDB.Get(ctx, key).Result()
		if err == nil {
			var result callbackResult
			if err := common.Unmarshal([]byte(val), &result); err != nil {
				return nil, err
			}
			return &result, nil
		}
		if err != redis.Nil {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("dramatiq image task timeout: %s", taskID)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func resultKey(taskID string) string { return resultKeyPrefix + taskID }

func isSuccessStatus(status string) bool {
	s := strings.ToUpper(strings.TrimSpace(status))
	return s == "SUCCESS" || s == "SUCCEEDED" || s == "COMPLETED"
}
