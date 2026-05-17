package dramatiq

import (
	"net/http"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/gin-gonic/gin"
)

func ImageCallback(c *gin.Context) {
	var payload callbackPayload
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	result := callbackResult{
		TaskID: strings.TrimSpace(payload.TaskID),
		Status: payload.Status,
		Error:  payload.ErrorMsg,
	}
	if result.TaskID == "" {
		result.TaskID = strings.TrimSpace(payload.TaskID2)
	}
	if result.TaskID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "taskId is required"})
		return
	}

	url, b64, errMsg := extractCallbackResult(payload.Result)
	result.URL = url
	result.B64JSON = b64
	if result.Error == "" {
		result.Error = errMsg
	}

	data, err := common.Marshal(result)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if common.RDB == nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "redis is not enabled"})
		return
	}
	if err := common.RDB.Set(c.Request.Context(), resultKey(result.TaskID), data, time.Hour).Err(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"success": true})
}

func extractCallbackResult(v any) (url string, b64 string, errMsg string) {
	switch t := v.(type) {
	case map[string]any:
		return extractResultMap(t)
	case []any:
		if len(t) == 0 {
			return "", "", ""
		}
		if m, ok := t[0].(map[string]any); ok {
			return extractResultMap(m)
		}
	case string:
		if strings.HasPrefix(t, "http") {
			return t, "", ""
		}
	}
	return "", "", ""
}

func extractResultMap(m map[string]any) (url string, b64 string, errMsg string) {
	if s, ok := m["img_url"].(string); ok {
		url = strings.TrimSpace(s)
	}
	for _, key := range []string{"error_msg", "display_error_msg"} {
		if s, ok := m[key].(string); ok && strings.TrimSpace(s) != "" {
			errMsg = strings.TrimSpace(s)
			break
		}
	}
	return url, b64, errMsg
}
