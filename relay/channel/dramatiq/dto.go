package dramatiq

type modelConfig struct {
	Adopt       string `json:"adopt"`
	Actor       string `json:"actor"`
	Queue       string `json:"queue"`
	TaskName    string `json:"task_name"`
	Workflow    string `json:"workflow_name"`
	TimeoutSecs int    `json:"timeout_seconds"`
}

type convertedRequest struct {
	TaskID         string         `json:"task_id"`
	Params         map[string]any `json:"params"`
	TimeoutSeconds int            `json:"timeout_seconds"`
}

type dramatiqMessage struct {
	QueueName        string         `json:"queue_name"`
	ActorName        string         `json:"actor_name"`
	Args             []any          `json:"args"`
	Kwargs           map[string]any `json:"kwargs"`
	Options          map[string]any `json:"options"`
	MessageID        string         `json:"message_id"`
	MessageTimestamp int64          `json:"message_timestamp"`
}

type callbackPayload struct {
	TaskID   string `json:"taskId"`
	TaskID2  string `json:"task_id"`
	TaskName string `json:"task_name"`
	Status   string `json:"status"`
	Result   any    `json:"result"`
	ErrorMsg string `json:"error_msg"`
}

type callbackResult struct {
	TaskID  string `json:"task_id"`
	Status  string `json:"status"`
	URL     string `json:"url,omitempty"`
	B64JSON string `json:"b64_json,omitempty"`
	Error   string `json:"error,omitempty"`
}
