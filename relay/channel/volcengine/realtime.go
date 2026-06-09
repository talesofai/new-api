package volcengine

import (
	"encoding/base64"
	"fmt"
	"strings"
	"sync"

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

func isDialogueEndpoint(baseUrl string) bool {
	return strings.Contains(baseUrl, "realtime/dialogue")
}

type realtimeState struct {
	mu        sync.RWMutex
	sessionID string
	inTurn    bool
}

func (s *realtimeState) SetSessionID(id string) {
	s.mu.Lock()
	s.sessionID = id
	s.mu.Unlock()
}

func (s *realtimeState) GetSessionID() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.sessionID
}

func (s *realtimeState) StartTurn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	wasInTurn := s.inTurn
	s.inTurn = true
	return !wasInTurn
}

func (s *realtimeState) EndTurn() {
	s.mu.Lock()
	s.inTurn = false
	s.mu.Unlock()
}

func realtimeEventID() string {
	raw := strings.ReplaceAll(uuid.New().String(), "-", "")
	if len(raw) > 12 {
		raw = raw[:12]
	}
	return "evt_" + raw
}

func sendClientEvent(c *gin.Context, conn *websocket.Conn, event map[string]interface{}) {
	data, err := common.Marshal(event)
	if err != nil {
		return
	}
	_ = helper.WssString(c, conn, string(data))
}

func sendBinaryJSON(conn *websocket.Conn, event EventType, sessionID string, payload []byte) error {
	msg := &Message{
		Version:       Version1,
		HeaderSize:    HeaderSize4,
		MsgType:       MsgTypeFullClientRequest,
		MsgTypeFlag:   MsgTypeFlagWithEvent,
		Serialization: SerializationJSON,
		Compression:   CompressionNone,
		EventType:     event,
		SessionID:     sessionID,
		Payload:       payload,
	}
	frame, err := msg.Marshal()
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, frame)
}

func sendBinaryAudio(conn *websocket.Conn, event EventType, sessionID string, audio []byte) error {
	msg := &Message{
		Version:     Version1,
		HeaderSize:  HeaderSize4,
		MsgType:     MsgTypeAudioOnlyClient,
		MsgTypeFlag: MsgTypeFlagWithEvent,
		Compression: CompressionNone,
		EventType:   event,
		SessionID:   sessionID,
		Payload:     audio,
	}
	frame, err := msg.Marshal()
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.BinaryMessage, frame)
}

// buildTTSConfig builds a StartSession payload for the bidirectional TTS API.
func buildTTSConfig(session map[string]interface{}) []byte {
	speaker := "zh_female_shuangkuaisisi_moon_bigtts"
	if v, ok := session["voice"].(string); ok && v != "" {
		speaker = v
	}
	format := "pcm"
	if v, ok := session["output_audio_format"].(string); ok && v != "" {
		format = v
	}
	sampleRate := float64(24000)
	if v, ok := session["sample_rate"].(float64); ok && v > 0 {
		sampleRate = v
	}

	config := map[string]interface{}{
		"event":     100,
		"namespace": "BidirectionalTTS",
		"req_params": map[string]interface{}{
			"speaker": speaker,
			"audio_params": map[string]interface{}{
				"format":      format,
				"sample_rate": int(sampleRate),
			},
		},
	}

	if additions := buildTTSAdditions(session); len(additions) > 0 {
		config["req_params"].(map[string]interface{})["additions"] = additions
	}

	data, _ := common.Marshal(config)
	return data
}

func buildTTSAdditions(session map[string]interface{}) map[string]interface{} {
	additions := map[string]interface{}{}
	if v, ok := session["disable_markdown_filter"].(bool); ok {
		additions["disable_markdown_filter"] = v
	}
	if v, ok := session["enable_language_detector"].(bool); ok {
		additions["enable_language_detector"] = v
	}
	if v, ok := session["explicit_language"].(string); ok && v != "" {
		additions["explicit_language"] = v
	}
	return additions
}

// buildDialogueConfig builds a StartSession payload for the realtime dialogue API.
func buildDialogueConfig(session map[string]interface{}) []byte {
	voice := "zh_female_cancan"
	if v, ok := session["voice"].(string); ok && v != "" {
		voice = v
	}
	instructions := ""
	if v, ok := session["instructions"].(string); ok {
		instructions = v
	}

	config := map[string]interface{}{
		"asr": map[string]interface{}{
			"language": "zh-CN",
		},
		"tts": map[string]interface{}{
			"speaker": voice,
			"audio_config": map[string]interface{}{
				"channel":     1,
				"format":      "pcm",
				"sample_rate": 16000,
				"bits":        16,
			},
		},
		"dialog": map[string]interface{}{
			"system_role": instructions,
		},
	}

	props := map[string]interface{}{}
	for _, k := range []string{"temperature", "top_p", "max_tokens"} {
		if v, ok := session[k]; ok {
			props[k] = v
		}
	}
	if len(props) > 0 {
		config["props"] = props
	}

	data, _ := common.Marshal(config)
	return data
}

// VolcengineRealtimeHandler bridges an OpenAI-protocol client WebSocket to
// the Volcengine realtime API (binary framing). TTS vs dialogue mode is
// determined by the channel's base_url configuration.
func VolcengineRealtimeHandler(c *gin.Context, info *relaycommon.RelayInfo) (*types.NewAPIError, *dto.RealtimeUsage) {
	if info == nil || info.ClientWs == nil || info.TargetWs == nil {
		return types.NewError(fmt.Errorf("invalid websocket connection"), types.ErrorCodeBadResponse), nil
	}

	info.IsStream = true
	clientConn := info.ClientWs
	targetConn := info.TargetWs
	dialogue := isDialogueEndpoint(info.ChannelBaseUrl)

	state := &realtimeState{}
	turnUsage := &dto.RealtimeUsage{}
	sumUsage := &dto.RealtimeUsage{}

	// --- connection handshake ---
	if err := sendBinaryJSON(targetConn, EventType_StartConnection, "", []byte("{}")); err != nil {
		return types.NewError(fmt.Errorf("StartConnection send failed: %v", err), types.ErrorCodeBadResponse), nil
	}
	connMsg, err := ReceiveMessage(targetConn)
	if err != nil {
		return types.NewError(fmt.Errorf("StartConnection recv failed: %v", err), types.ErrorCodeBadResponse), nil
	}
	if connMsg.EventType != EventType_ConnectionStarted {
		return types.NewError(fmt.Errorf("expected ConnectionStarted, got %v", connMsg.EventType), types.ErrorCodeBadResponse), nil
	}

	sendClientEvent(c, clientConn, map[string]interface{}{
		"event_id": realtimeEventID(),
		"type":     "session.created",
		"session":  map[string]interface{}{},
	})

	clientClosed := make(chan struct{})
	targetClosed := make(chan struct{})
	errChan := make(chan error, 2)

	gopool.Go(func() {
		<-c.Done()
		clientConn.Close()
		targetConn.Close()
	})

	// Client → Upstream  (OpenAI JSON → Volcengine binary)
	gopool.Go(func() {
		defer close(clientClosed)
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in client reader: %v", r)
			}
		}()
		for {
			msgType, message, readErr := clientConn.ReadMessage()
			if readErr != nil {
				if !websocket.IsCloseError(readErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					errChan <- fmt.Errorf("error reading client: %v", readErr)
				}
				return
			}

			if msgType == websocket.BinaryMessage {
				if sendErr := sendBinaryAudio(targetConn, EventType_TaskRequest, state.GetSessionID(), message); sendErr != nil {
					errChan <- fmt.Errorf("error writing binary audio: %v", sendErr)
					return
				}
				continue
			}

			var event map[string]interface{}
			if parseErr := common.Unmarshal(message, &event); parseErr != nil {
				continue
			}
			evtType, _ := event["type"].(string)
			sid := state.GetSessionID()

			switch evtType {
			case "session.update":
				session, _ := event["session"].(map[string]interface{})
				if session == nil {
					session = map[string]interface{}{}
				}
				newSid := uuid.New().String()
				state.SetSessionID(newSid)
				var cfg []byte
				if dialogue {
					cfg = buildDialogueConfig(session)
				} else {
					cfg = buildTTSConfig(session)
				}
				if sendErr := sendBinaryJSON(targetConn, EventType_StartSession, newSid, cfg); sendErr != nil {
					errChan <- fmt.Errorf("StartSession send failed: %v", sendErr)
					return
				}

			case "input_audio_buffer.append":
				if !dialogue {
					continue
				}
				audioB64, _ := event["audio"].(string)
				if audioB64 == "" {
					continue
				}
				audioBytes, decErr := base64.StdEncoding.DecodeString(audioB64)
				if decErr != nil {
					continue
				}
				if sendErr := sendBinaryAudio(targetConn, EventType_TaskRequest, sid, audioBytes); sendErr != nil {
					errChan <- fmt.Errorf("error sending audio: %v", sendErr)
					return
				}

			case "input_audio_buffer.commit", "input_audio_buffer.clear":
				// No direct equivalent

			case "conversation.item.create":
				text := extractTextFromItem(event)
				if text == "" {
					continue
				}
				if dialogue {
					payload, _ := common.Marshal(map[string]interface{}{"text": text})
					if sendErr := sendBinaryJSON(targetConn, EventType_UserTextQuery, sid, payload); sendErr != nil {
						errChan <- fmt.Errorf("UserTextQuery send failed: %v", sendErr)
						return
					}
				} else {
					payload, _ := common.Marshal(map[string]interface{}{
						"event":     200,
						"namespace": "BidirectionalTTS",
						"req_params": map[string]interface{}{
							"text": text,
						},
					})
					if sendErr := sendBinaryJSON(targetConn, EventType_TaskRequest, sid, payload); sendErr != nil {
						errChan <- fmt.Errorf("TaskRequest send failed: %v", sendErr)
						return
					}
				}

			case "response.create":
				if !dialogue {
					// For TTS mode, response.create after text means we're done sending text.
					// Send FinishSession to trigger final audio flush.
					_ = sendBinaryJSON(targetConn, EventType_FinishSession, sid, []byte("{}"))
				}

			case "response.cancel":
				_ = sendBinaryJSON(targetConn, EventType_CancelSession, sid, []byte("{}"))
			}
		}
	})

	// Upstream → Client  (Volcengine binary → OpenAI JSON)
	gopool.Go(func() {
		defer close(targetClosed)
		defer func() {
			if r := recover(); r != nil {
				errChan <- fmt.Errorf("panic in target reader: %v", r)
			}
		}()
		for {
			msg, readErr := ReceiveMessage(targetConn)
			if readErr != nil {
				if !websocket.IsCloseError(readErr, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					errChan <- fmt.Errorf("error reading target: %v", readErr)
				}
				return
			}
			info.SetFirstResponseTime()

			switch msg.EventType {
			case EventType_SessionStarted:
				state.SetSessionID(msg.SessionID)
				sendClientEvent(c, clientConn, map[string]interface{}{
					"event_id": realtimeEventID(),
					"type":     "session.updated",
					"session":  map[string]interface{}{},
				})

			case EventType_SessionFailed:
				sendClientEvent(c, clientConn, map[string]interface{}{
					"event_id": realtimeEventID(),
					"type":     "error",
					"error": map[string]interface{}{
						"type":    "server_error",
						"message": string(msg.Payload),
					},
				})

			case EventType_SessionCanceled:
				sendClientEvent(c, clientConn, map[string]interface{}{
					"event_id": realtimeEventID(),
					"type":     "response.done",
					"response": map[string]interface{}{"status": "cancelled"},
				})
				state.EndTurn()

			case EventType_ASRResponse:
				if !dialogue {
					continue
				}
				var d map[string]interface{}
				if common.Unmarshal(msg.Payload, &d) == nil {
					if text, _ := d["text"].(string); text != "" {
						sendClientEvent(c, clientConn, map[string]interface{}{
							"event_id":   realtimeEventID(),
							"type":       "conversation.item.input_audio_transcription.completed",
							"transcript": text,
						})
					}
				}

			case EventType_ASRInfo:
				if !dialogue {
					continue
				}
				var d map[string]interface{}
				if common.Unmarshal(msg.Payload, &d) == nil {
					if text, _ := d["text"].(string); text != "" {
						sendClientEvent(c, clientConn, map[string]interface{}{
							"event_id": realtimeEventID(),
							"type":     "conversation.item.input_audio_transcription.delta",
							"delta":    text,
						})
					}
				}

			case EventType_ChatResponse:
				if !dialogue {
					continue
				}
				if state.StartTurn() {
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.created",
						"response": map[string]interface{}{"status": "in_progress"},
					})
				}
				var d map[string]interface{}
				if common.Unmarshal(msg.Payload, &d) == nil {
					if text, _ := d["text"].(string); text != "" {
						sendClientEvent(c, clientConn, map[string]interface{}{
							"event_id": realtimeEventID(),
							"type":     "response.audio_transcript.delta",
							"delta":    text,
						})
						turnUsage.OutputTokens += len([]rune(text))
						turnUsage.OutputTokenDetails.TextTokens += len([]rune(text))
						turnUsage.TotalTokens += len([]rune(text))
					}
				}

			case EventType_ChatEnded:
				if !dialogue {
					continue
				}
				sendClientEvent(c, clientConn, map[string]interface{}{
					"event_id": realtimeEventID(),
					"type":     "response.audio_transcript.done",
				})

			case EventType_TTSSentenceStart:
				if state.StartTurn() {
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.created",
						"response": map[string]interface{}{"status": "in_progress"},
					})
				}

			case EventType_TTSResponse:
				if len(msg.Payload) > 0 {
					audioB64 := base64.StdEncoding.EncodeToString(msg.Payload)
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.audio.delta",
						"delta":    audioB64,
					})
					audioTokens := len(msg.Payload) / 640
					if audioTokens < 1 {
						audioTokens = 1
					}
					turnUsage.OutputTokens += audioTokens
					turnUsage.OutputTokenDetails.AudioTokens += audioTokens
					turnUsage.TotalTokens += audioTokens
				}

			case EventType_TTSSentenceEnd:
				var d map[string]interface{}
				if common.Unmarshal(msg.Payload, &d) == nil {
					if t, _ := d["text"].(string); t != "" {
						sendClientEvent(c, clientConn, map[string]interface{}{
							"event_id": realtimeEventID(),
							"type":     "response.audio_transcript.delta",
							"delta":    t,
						})
						turnUsage.OutputTokens += len([]rune(t))
						turnUsage.OutputTokenDetails.TextTokens += len([]rune(t))
						turnUsage.TotalTokens += len([]rune(t))
					}
				}

			case EventType_TTSEnded:
				if dialogue {
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.audio.done",
					})
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.done",
						"response": map[string]interface{}{"status": "completed"},
					})
					state.EndTurn()
					_ = volcenginePreConsumeUsage(c, info, turnUsage, sumUsage)
					turnUsage = &dto.RealtimeUsage{}
				}

			case EventType_SessionFinished:
				if !dialogue {
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.audio.done",
					})
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "response.done",
						"response": map[string]interface{}{"status": "completed"},
					})
					state.EndTurn()
					_ = volcenginePreConsumeUsage(c, info, turnUsage, sumUsage)
					turnUsage = &dto.RealtimeUsage{}
				}
				logger.LogInfo(c, "volcengine realtime session finished")

			case EventType_UsageResponse:
				logger.LogInfo(c, fmt.Sprintf("volcengine realtime usage: %s", string(msg.Payload)))

			case EventType_ConnectionFailed:
				sendClientEvent(c, clientConn, map[string]interface{}{
					"event_id": realtimeEventID(),
					"type":     "error",
					"error": map[string]interface{}{
						"type":    "connection_error",
						"message": string(msg.Payload),
					},
				})

			default:
				if msg.MsgType == MsgTypeError {
					sendClientEvent(c, clientConn, map[string]interface{}{
						"event_id": realtimeEventID(),
						"type":     "error",
						"error": map[string]interface{}{
							"type":    "server_error",
							"code":    msg.ErrorCode,
							"message": string(msg.Payload),
						},
					})
				} else {
					logger.LogInfo(c, fmt.Sprintf("volcengine realtime unhandled: %v", msg))
				}
			}
		}
	})

	select {
	case <-clientClosed:
	case <-targetClosed:
	case err := <-errChan:
		logger.LogError(c, "volcengine realtime error: "+err.Error())
	case <-c.Done():
	}

	clientConn.Close()
	targetConn.Close()
	<-clientClosed
	<-targetClosed

	if turnUsage.TotalTokens > 0 {
		_ = volcenginePreConsumeUsage(c, info, turnUsage, sumUsage)
	}

	return nil, sumUsage
}

func volcenginePreConsumeUsage(ctx *gin.Context, info *relaycommon.RelayInfo, usage *dto.RealtimeUsage, totalUsage *dto.RealtimeUsage) error {
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

func extractTextFromItem(event map[string]interface{}) string {
	item, _ := event["item"].(map[string]interface{})
	if item == nil {
		return ""
	}
	content, _ := item["content"].([]interface{})
	for _, c := range content {
		if cm, ok := c.(map[string]interface{}); ok {
			if t, ok := cm["text"].(string); ok && t != "" {
				return t
			}
		}
	}
	return ""
}
