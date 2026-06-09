package ali

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

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

// openaiToQwen strips internal fields from session.update events and ensures
// event_id presence. Dashscope's realtime API (for qwen-omni-*-realtime models)
// is OpenAI-compatible, so all other events pass through unchanged.
func openaiToQwen(message []byte) []byte {
	var event map[string]interface{}
	if err := common.Unmarshal(message, &event); err != nil {
		return message
	}

	evtType, _ := event["type"].(string)

	if evtType == "session.update" {
		return stripSessionInternalFields(event)
	}

	if _, ok := event["event_id"]; !ok {
		event["event_id"] = eventID()
		converted, _ := common.Marshal(event)
		return converted
	}
	return message
}

// stripSessionInternalFields removes fields that should not be forwarded
// to the upstream (security-sensitive or proxy-internal).
func stripSessionInternalFields(event map[string]interface{}) []byte {
	stripped := map[string]bool{
		"llm_api_base": true,
		"llm_api_key":  true,
		"llm_model":    true,
		"preset_key":   true,
	}

	if session, ok := event["session"].(map[string]interface{}); ok {
		for k := range stripped {
			delete(session, k)
		}
	}

	if _, ok := event["event_id"]; !ok {
		event["event_id"] = eventID()
	}

	converted, _ := common.Marshal(event)
	return converted
}

func sendClientEvent(c *gin.Context, conn *websocket.Conn, mu *sync.Mutex, event map[string]interface{}) {
	data, err := common.Marshal(event)
	if err != nil {
		return
	}
	mu.Lock()
	defer mu.Unlock()
	_ = helper.WssString(c, conn, string(data))
}

// handleVoiceCreate calls the Dashscope voice enrollment REST API and sends
// the result back to the client. Runs async so the realtime session is not
// blocked during enrollment.
func handleVoiceCreate(c *gin.Context, info *relaycommon.RelayInfo, event map[string]interface{}, clientConn *websocket.Conn, clientMu *sync.Mutex) {
	reqEventID, _ := event["event_id"].(string)
	if reqEventID == "" {
		reqEventID = eventID()
	}

	audioURL, _ := event["url"].(string)
	audioData, _ := event["audio"].(string)
	if audioURL == "" && audioData == "" {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": "url or audio field is required",
			},
		})
		return
	}

	enrollModel := "qwen-voice-enrollment"
	if m, _ := event["model"].(string); m != "" {
		enrollModel = m
	}

	targetModel := info.UpstreamModelName
	if tm, _ := event["target_model"].(string); tm != "" {
		targetModel = tm
	}

	audioField := audioURL
	if audioField == "" {
		audioField = audioData
	}

	enrollInput := map[string]interface{}{
		"action":       "create",
		"target_model": targetModel,
		"audio":        map[string]interface{}{"data": audioField},
	}
	if name, _ := event["name"].(string); name != "" {
		enrollInput["preferred_name"] = name
	}

	reqBody, err := common.Marshal(map[string]interface{}{
		"model": enrollModel,
		"input": enrollInput,
	})
	if err != nil {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": "failed to build request: " + err.Error(),
			},
		})
		return
	}

	enrollURL := fmt.Sprintf("%s/api/v1/services/audio/tts/customization", info.ChannelBaseUrl)
	httpReq, err := http.NewRequestWithContext(c, http.MethodPost, enrollURL, bytes.NewReader(reqBody))
	if err != nil {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": err.Error(),
			},
		})
		return
	}
	httpReq.Header.Set("Authorization", "Bearer "+info.ApiKey)
	httpReq.Header.Set("Content-Type", "application/json")

	httpClient := &http.Client{Timeout: 30 * time.Second}
	resp, err := httpClient.Do(httpReq)
	if err != nil {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": err.Error(),
			},
		})
		return
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": err.Error(),
			},
		})
		return
	}

	if resp.StatusCode != http.StatusOK {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(respBody)),
			},
		})
		return
	}

	var enrollResp map[string]interface{}
	if err := common.Unmarshal(respBody, &enrollResp); err != nil {
		sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
			"type":     "error",
			"event_id": reqEventID,
			"error": map[string]interface{}{
				"type":    "voice_enrollment_error",
				"message": "invalid response: " + err.Error(),
			},
		})
		return
	}

	output, _ := enrollResp["output"].(map[string]interface{})
	voiceID, _ := output["voice"].(string)
	if voiceID == "" {
		voiceID, _ = output["voice_id"].(string)
	}

	voiceObj := map[string]interface{}{"id": voiceID}
	if name, _ := event["name"].(string); name != "" {
		voiceObj["name"] = name
	}

	logger.LogInfo(c, fmt.Sprintf("voice enrollment complete, voice_id=%s", voiceID))
	sendClientEvent(c, clientConn, clientMu, map[string]interface{}{
		"type":     "voice.created",
		"event_id": reqEventID,
		"voice":    voiceObj,
	})
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
	var clientMu sync.Mutex
	var usageMu sync.Mutex

	// Close both connections when context is cancelled, which unblocks any
	// blocking ReadMessage calls in the goroutines below.
	gopool.Go(func() {
		<-c.Done()
		clientConn.Close()
		targetConn.Close()
	})

	// Client -> Target (with OpenAI -> Dashscope conversion)
	gopool.Go(func() {
		defer close(clientClosed)
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

			// Intercept proxy-only voice events — never forward to upstream
			var typeCheck struct {
				Type string `json:"type"`
			}
			if common.Unmarshal(message, &typeCheck) == nil && strings.HasPrefix(typeCheck.Type, "voice.") {
				var event map[string]interface{}
				if common.Unmarshal(message, &event) == nil {
					switch typeCheck.Type {
					case "voice.create":
						gopool.Go(func() {
							handleVoiceCreate(c, info, event, clientConn, &clientMu)
						})
					default:
						sendClientEvent(c, clientConn, &clientMu, map[string]interface{}{
							"type":     "error",
							"event_id": eventID(),
							"error": map[string]interface{}{
								"type":    "invalid_event",
								"message": fmt.Sprintf("unsupported voice event: %s", typeCheck.Type),
							},
						})
					}
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
				usageMu.Lock()
				localUsage.TotalTokens += textToken + audioToken
				localUsage.InputTokens += textToken + audioToken
				localUsage.InputTokenDetails.TextTokens += textToken
				localUsage.InputTokenDetails.AudioTokens += audioToken
				usageMu.Unlock()
			}

			converted := openaiToQwen(message)
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
		defer close(targetClosed)
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
				return
			}
			info.SetFirstResponseTime()

			realtimeEvent := &dto.RealtimeEvent{}
			if unmarshalErr := common.Unmarshal(message, realtimeEvent); unmarshalErr == nil {
				if realtimeEvent.Type == dto.RealtimeEventTypeResponseDone && realtimeEvent.Response != nil {
					realtimeUsage := realtimeEvent.Response.Usage
					if realtimeUsage != nil {
						usageMu.Lock()
						usage.TotalTokens += realtimeUsage.TotalTokens
						usage.InputTokens += realtimeUsage.InputTokens
						usage.OutputTokens += realtimeUsage.OutputTokens
						usage.InputTokenDetails.AudioTokens += realtimeUsage.InputTokenDetails.AudioTokens
						usage.InputTokenDetails.CachedTokens += realtimeUsage.InputTokenDetails.CachedTokens
						usage.InputTokenDetails.TextTokens += realtimeUsage.InputTokenDetails.TextTokens
						usage.OutputTokenDetails.AudioTokens += realtimeUsage.OutputTokenDetails.AudioTokens
						usage.OutputTokenDetails.TextTokens += realtimeUsage.OutputTokenDetails.TextTokens
						preConsumeErr := dashscopePreConsumeUsage(c, info, usage, sumUsage)
						usage = &dto.RealtimeUsage{}
						localUsage = &dto.RealtimeUsage{}
						usageMu.Unlock()
						if preConsumeErr != nil {
							errChan <- fmt.Errorf("error consume usage: %v", preConsumeErr)
							return
						}
					} else {
						textToken, audioToken, countErr := service.CountTokenRealtime(info, *realtimeEvent, info.UpstreamModelName)
						if countErr != nil {
							errChan <- fmt.Errorf("error counting output token: %v", countErr)
							return
						}
						logger.LogInfo(c, fmt.Sprintf("dashscope target type: %s, textToken: %d, audioToken: %d", realtimeEvent.Type, textToken, audioToken))
						usageMu.Lock()
						localUsage.TotalTokens += textToken + audioToken
						info.IsFirstRequest = false
						localUsage.InputTokens += textToken + audioToken
						localUsage.InputTokenDetails.TextTokens += textToken
						localUsage.InputTokenDetails.AudioTokens += audioToken
						preConsumeErr := dashscopePreConsumeUsage(c, info, localUsage, sumUsage)
						localUsage = &dto.RealtimeUsage{}
						usageMu.Unlock()
						if preConsumeErr != nil {
							errChan <- fmt.Errorf("error consume usage: %v", preConsumeErr)
							return
						}
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
					usageMu.Lock()
					localUsage.TotalTokens += textToken + audioToken
					localUsage.OutputTokens += textToken + audioToken
					localUsage.OutputTokenDetails.TextTokens += textToken
					localUsage.OutputTokenDetails.AudioTokens += audioToken
					usageMu.Unlock()
				}
			}

			clientMu.Lock()
			writeErr := helper.WssString(c, clientConn, string(message))
			clientMu.Unlock()
			if writeErr != nil {
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

	clientConn.Close()
	targetConn.Close()
	<-clientClosed
	<-targetClosed

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
