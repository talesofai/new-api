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
	session, _ := event["session"].(map[string]interface{})
	if session == nil {
		session = map[string]interface{}{}
	}

	stripped := map[string]bool{
		"llm_api_base": true,
		"llm_api_key":  true,
		"llm_model":    true,
		"preset_key":   true,
	}

	qwenSession := map[string]interface{}{}
	for k, v := range session {
		if stripped[k] {
			continue
		}
		qwenSession[k] = v
	}

	result := map[string]interface{}{
		"event_id": eventID(),
		"type":     "session.update",
		"session":  qwenSession,
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
