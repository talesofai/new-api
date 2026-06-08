package ali

import (
	"fmt"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/dto"
	"github.com/QuantumNous/new-api/logger"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relay/helper"
	"github.com/QuantumNous/new-api/service"
	"github.com/QuantumNous/new-api/types"

	"github.com/bytedance/gopkg/util/gopool"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// eventID generates a short event ID in the form "evt_xxxxxxxxxxxx".
func eventID() string {
	raw := strings.ReplaceAll(uuid.New().String(), "-", "")
	if len(raw) > 12 {
		raw = raw[:12]
	}
	return "evt_" + raw
}

// getStringOrDefault returns the string value for key in m, or defaultVal if
// the key is absent or not a string.
func getStringOrDefault(m map[string]interface{}, key, defaultVal string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok && s != "" {
			return s
		}
	}
	return defaultVal
}

// getNumberOrDefault returns the numeric value for key in m, or defaultVal if
// the key is absent or not a number.
func getNumberOrDefault(m map[string]interface{}, key string, defaultVal float64) float64 {
	if v, ok := m[key]; ok {
		switch n := v.(type) {
		case float64:
			return n
		case int:
			return float64(n)
		}
	}
	return defaultVal
}

type dashscopeModelType int

const (
	dashscopeModelTTS dashscopeModelType = iota
	dashscopeModelASR
	dashscopeModelOmni
)

func detectDashscopeModelType(modelName string) dashscopeModelType {
	lower := strings.ToLower(modelName)
	if strings.Contains(lower, "tts") {
		return dashscopeModelTTS
	}
	if strings.Contains(lower, "asr") {
		return dashscopeModelASR
	}
	return dashscopeModelOmni
}

// openaiToQwen converts an OpenAI realtime client event to Dashscope format.
// For TTS models, conversation.item.create and response.create are remapped to
// input_text_buffer events. For ASR and Omni models, these events pass through
// unchanged since Dashscope natively supports the OpenAI event names.
// Returns the converted message bytes, or nil if the event should be dropped.
func openaiToQwen(message []byte, modelName string) []byte {
	var event map[string]interface{}
	if err := common.Unmarshal(message, &event); err != nil {
		return message // can't parse, passthrough
	}

	evtType, _ := event["type"].(string)
	modelType := detectDashscopeModelType(modelName)

	switch evtType {
	case "session.update":
		return convertSessionUpdate(event, modelType)
	case "conversation.item.create":
		if modelType == dashscopeModelTTS {
			return convertItemCreate(event)
		}
	case "response.create":
		if modelType == dashscopeModelTTS {
			return convertResponseCreate()
		}
	}

	// Passthrough: ensure event_id exists
	if _, ok := event["event_id"]; !ok {
		event["event_id"] = eventID()
		converted, _ := common.Marshal(event)
		return converted
	}
	return message
}

// convertSessionUpdate remaps an OpenAI session.update event to Dashscope
// format, adjusting fields based on the model type:
//   - TTS: voice, mode, response_format, sample_rate
//   - ASR: input_audio_format, sample_rate, turn_detection
//   - Omni: modalities, voice, input/output_audio_format, turn_detection, instructions, tools
//
// Internal fields (llm_api_base, llm_api_key, llm_model, preset_key) are always stripped.
func convertSessionUpdate(event map[string]interface{}, modelType dashscopeModelType) []byte {
	session, _ := event["session"].(map[string]interface{})
	if session == nil {
		session = map[string]interface{}{}
	}

	internalFields := map[string]bool{
		"llm_api_base": true,
		"llm_api_key":  true,
		"llm_model":    true,
		"preset_key":   true,
	}

	var qwenSession map[string]interface{}

	switch modelType {
	case dashscopeModelTTS:
		qwenSession = map[string]interface{}{
			"voice":           getStringOrDefault(session, "voice", "Cherry"),
			"mode":            getStringOrDefault(session, "mode", "server_commit"),
			"response_format": getStringOrDefault(session, "response_format", "pcm"),
			"sample_rate":     getNumberOrDefault(session, "sample_rate", 24000),
		}
		for _, k := range []string{"voice", "mode", "response_format", "sample_rate"} {
			internalFields[k] = true
		}

	case dashscopeModelASR:
		qwenSession = map[string]interface{}{
			"input_audio_format": getStringOrDefault(session, "input_audio_format", "pcm"),
			"sample_rate":        getNumberOrDefault(session, "sample_rate", 16000),
		}
		if v, ok := session["turn_detection"]; ok {
			qwenSession["turn_detection"] = v
		}
		if v, ok := session["input_audio_transcription"]; ok {
			qwenSession["input_audio_transcription"] = v
		}
		for _, k := range []string{"input_audio_format", "sample_rate",
			"voice", "mode", "response_format", "output_audio_format"} {
			internalFields[k] = true
		}

	default: // dashscopeModelOmni
		qwenSession = map[string]interface{}{}
		if _, ok := session["modalities"]; !ok {
			qwenSession["modalities"] = []string{"text", "audio"}
		}
		if _, ok := session["voice"]; !ok {
			qwenSession["voice"] = "Cherry"
		}
		if _, ok := session["input_audio_format"]; !ok {
			qwenSession["input_audio_format"] = "pcm"
		}
		if _, ok := session["output_audio_format"]; !ok {
			qwenSession["output_audio_format"] = "pcm"
		}
		for _, k := range []string{"mode", "response_format"} {
			internalFields[k] = true
		}
	}

	for k, v := range session {
		if internalFields[k] {
			continue
		}
		if _, exists := qwenSession[k]; !exists {
			qwenSession[k] = v
		}
	}

	result := map[string]interface{}{
		"event_id": eventID(),
		"type":     "session.update",
		"session":  qwenSession,
	}
	converted, _ := common.Marshal(result)
	return converted
}

// convertItemCreate converts an OpenAI conversation.item.create event to a
// Dashscope input_text_buffer.append event, extracting text from the item
// content array.
func convertItemCreate(event map[string]interface{}) []byte {
	item, _ := event["item"].(map[string]interface{})
	if item == nil {
		return nil
	}
	content, _ := item["content"].([]interface{})
	var texts []string
	for _, c := range content {
		if cm, ok := c.(map[string]interface{}); ok {
			if t, ok := cm["text"].(string); ok && t != "" {
				texts = append(texts, t)
			}
		}
	}
	if len(texts) == 0 {
		return nil
	}
	result := map[string]interface{}{
		"event_id": eventID(),
		"type":     "input_text_buffer.append",
		"text":     strings.Join(texts, " "),
	}
	converted, _ := common.Marshal(result)
	return converted
}

// convertResponseCreate converts an OpenAI response.create event to a
// Dashscope input_text_buffer.commit event.
func convertResponseCreate() []byte {
	result := map[string]interface{}{
		"event_id": eventID(),
		"type":     "input_text_buffer.commit",
	}
	converted, _ := common.Marshal(result)
	return converted
}

// DashscopeRealtimeHandler handles the WebSocket relay between a client using
// the OpenAI realtime protocol and a Dashscope/Qwen upstream. Client messages
// are converted from OpenAI format to Dashscope format; upstream messages are
// passed through unchanged (Dashscope already aligns with OpenAI format).
func DashscopeRealtimeHandler(c *gin.Context, info *relaycommon.RelayInfo) (*types.NewAPIError, *dto.RealtimeUsage) {
	if info == nil || info.ClientWs == nil || info.TargetWs == nil {
		return types.NewError(fmt.Errorf("invalid websocket connection"), types.ErrorCodeBadResponse), nil
	}

	info.IsStream = true
	clientConn := info.ClientWs
	targetConn := info.TargetWs

	clientClosed := make(chan struct{})
	targetClosed := make(chan struct{})
	errChan := make(chan error, 2)

	usage := &dto.RealtimeUsage{}
	localUsage := &dto.RealtimeUsage{}
	sumUsage := &dto.RealtimeUsage{}

	// Close both connections when context is cancelled, which unblocks any
	// blocking ReadMessage calls in the goroutines below.
	gopool.Go(func() {
		<-c.Done()
		clientConn.Close()
		targetConn.Close()
	})

	// Client -> Target (with OpenAI -> Dashscope conversion)
	gopool.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in client reader: %v", r)
			}
		}()
		for {
			msgType, message, err := clientConn.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					errChan <- fmt.Errorf("error reading from client: %v", err)
				}
				close(clientClosed)
				return
			}

			// Binary messages (audio) pass through unchanged
			if msgType == websocket.BinaryMessage {
				if writeErr := targetConn.WriteMessage(msgType, message); writeErr != nil {
					errChan <- fmt.Errorf("error writing binary to target: %v", writeErr)
					return
				}
				continue
			}

			// Count input tokens from the original event before conversion
			realtimeEvent := &dto.RealtimeEvent{}
			if unmarshalErr := common.Unmarshal(message, realtimeEvent); unmarshalErr == nil {
				if realtimeEvent.Type == dto.RealtimeEventTypeSessionUpdate {
					if realtimeEvent.Session != nil && realtimeEvent.Session.Tools != nil {
						info.RealtimeTools = realtimeEvent.Session.Tools
					}
				}
				textToken, audioToken, countErr := service.CountTokenRealtime(info, *realtimeEvent, info.UpstreamModelName)
				if countErr != nil {
					errChan <- fmt.Errorf("error counting input token: %v", countErr)
					return
				}
				logger.LogInfo(c, fmt.Sprintf("dashscope client type: %s, textToken: %d, audioToken: %d", realtimeEvent.Type, textToken, audioToken))
				localUsage.TotalTokens += textToken + audioToken
				localUsage.InputTokens += textToken + audioToken
				localUsage.InputTokenDetails.TextTokens += textToken
				localUsage.InputTokenDetails.AudioTokens += audioToken
			}

			// Convert OpenAI -> Dashscope
			converted := openaiToQwen(message, info.UpstreamModelName)
			if converted == nil {
				continue // event was dropped (e.g. empty text)
			}

			if writeErr := helper.WssString(c, targetConn, string(converted)); writeErr != nil {
				errChan <- fmt.Errorf("error writing to target: %v", writeErr)
				return
			}
		}
	})

	// Target -> Client (passthrough, Dashscope events are OpenAI-compatible)
	gopool.Go(func() {
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in target reader: %v", r)
			}
		}()
		for {
			_, message, err := targetConn.ReadMessage()
			if err != nil {
				if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					errChan <- fmt.Errorf("error reading from target: %v", err)
				}
				close(targetClosed)
				return
			}
			info.SetFirstResponseTime()

			realtimeEvent := &dto.RealtimeEvent{}
			if unmarshalErr := common.Unmarshal(message, realtimeEvent); unmarshalErr == nil {
				if realtimeEvent.Type == dto.RealtimeEventTypeResponseDone && realtimeEvent.Response != nil {
					realtimeUsage := realtimeEvent.Response.Usage
					if realtimeUsage != nil {
						usage.TotalTokens += realtimeUsage.TotalTokens
						usage.InputTokens += realtimeUsage.InputTokens
						usage.OutputTokens += realtimeUsage.OutputTokens
						usage.InputTokenDetails.AudioTokens += realtimeUsage.InputTokenDetails.AudioTokens
						usage.InputTokenDetails.CachedTokens += realtimeUsage.InputTokenDetails.CachedTokens
						usage.InputTokenDetails.TextTokens += realtimeUsage.InputTokenDetails.TextTokens
						usage.OutputTokenDetails.AudioTokens += realtimeUsage.OutputTokenDetails.AudioTokens
						usage.OutputTokenDetails.TextTokens += realtimeUsage.OutputTokenDetails.TextTokens

						preConsumeErr := dashscopePreConsumeUsage(c, info, usage, sumUsage)
						if preConsumeErr != nil {
							errChan <- fmt.Errorf("error consume usage: %v", preConsumeErr)
							return
						}
						usage = &dto.RealtimeUsage{}
						localUsage = &dto.RealtimeUsage{}
					} else {
						textToken, audioToken, countErr := service.CountTokenRealtime(info, *realtimeEvent, info.UpstreamModelName)
						if countErr != nil {
							errChan <- fmt.Errorf("error counting output token: %v", countErr)
							return
						}
						logger.LogInfo(c, fmt.Sprintf("dashscope target type: %s, textToken: %d, audioToken: %d", realtimeEvent.Type, textToken, audioToken))
						localUsage.TotalTokens += textToken + audioToken
						info.IsFirstRequest = false
						localUsage.InputTokens += textToken + audioToken
						localUsage.InputTokenDetails.TextTokens += textToken
						localUsage.InputTokenDetails.AudioTokens += audioToken

						preConsumeErr := dashscopePreConsumeUsage(c, info, localUsage, sumUsage)
						if preConsumeErr != nil {
							errChan <- fmt.Errorf("error consume usage: %v", preConsumeErr)
							return
						}
						localUsage = &dto.RealtimeUsage{}
					}
					logger.LogInfo(c, fmt.Sprintf("dashscope realtime sumUsage: %v", sumUsage))
				} else if realtimeEvent.Type == dto.RealtimeEventTypeSessionUpdated || realtimeEvent.Type == dto.RealtimeEventTypeSessionCreated {
					realtimeSession := realtimeEvent.Session
					if realtimeSession != nil {
						info.InputAudioFormat = common.GetStringIfEmpty(realtimeSession.InputAudioFormat, info.InputAudioFormat)
						info.OutputAudioFormat = common.GetStringIfEmpty(realtimeSession.OutputAudioFormat, info.OutputAudioFormat)
					}
				} else {
					textToken, audioToken, countErr := service.CountTokenRealtime(info, *realtimeEvent, info.UpstreamModelName)
					if countErr != nil {
						errChan <- fmt.Errorf("error counting output token: %v", countErr)
						return
					}
					logger.LogInfo(c, fmt.Sprintf("dashscope target type: %s, textToken: %d, audioToken: %d", realtimeEvent.Type, textToken, audioToken))
					localUsage.TotalTokens += textToken + audioToken
					localUsage.OutputTokens += textToken + audioToken
					localUsage.OutputTokenDetails.TextTokens += textToken
					localUsage.OutputTokenDetails.AudioTokens += audioToken
				}
			}

			if writeErr := helper.WssString(c, clientConn, string(message)); writeErr != nil {
				errChan <- fmt.Errorf("error writing to client: %v", writeErr)
				return
			}
		}
	})

	select {
	case <-clientClosed:
	case <-targetClosed:
	case err := <-errChan:
		logger.LogError(c, "dashscope realtime error: "+err.Error())
	case <-c.Done():
	}

	// Flush any remaining usage
	if usage.TotalTokens != 0 {
		_ = dashscopePreConsumeUsage(c, info, usage, sumUsage)
	}
	if localUsage.TotalTokens != 0 {
		_ = dashscopePreConsumeUsage(c, info, localUsage, sumUsage)
	}

	return nil, sumUsage
}

// dashscopePreConsumeUsage accumulates per-turn usage into totalUsage and
// triggers the pre-consume billing flow. This mirrors the openai package's
// preConsumeUsage but lives in the ali package to avoid cross-package
// dependency on an unexported function.
func dashscopePreConsumeUsage(ctx *gin.Context, info *relaycommon.RelayInfo, usage *dto.RealtimeUsage, totalUsage *dto.RealtimeUsage) error {
	if usage == nil || totalUsage == nil {
		return fmt.Errorf("invalid usage pointer")
	}

	totalUsage.TotalTokens += usage.TotalTokens
	totalUsage.InputTokens += usage.InputTokens
	totalUsage.OutputTokens += usage.OutputTokens
	totalUsage.InputTokenDetails.CachedTokens += usage.InputTokenDetails.CachedTokens
	totalUsage.InputTokenDetails.TextTokens += usage.InputTokenDetails.TextTokens
	totalUsage.InputTokenDetails.AudioTokens += usage.InputTokenDetails.AudioTokens
	totalUsage.OutputTokenDetails.TextTokens += usage.OutputTokenDetails.TextTokens
	totalUsage.OutputTokenDetails.AudioTokens += usage.OutputTokenDetails.AudioTokens

	return service.PreWssConsumeQuota(ctx, info, usage)
}
