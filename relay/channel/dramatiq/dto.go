package dramatiq

type modelConfig struct {
	Adopt       string `json:"adopt"`
	Actor       string `json:"actor"`
	BrokerURL   string `json:"dramatiq_broker_url"`
	Namespace   string `json:"dramatiq_namespace"`
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
	TaskName string `json:"task_name"`
	Status   string `json:"status"`
	Result   any    `json:"result"`
}

type callbackResult struct {
	TaskID string `json:"task_id"`
	Status string `json:"status"`
	URL    string `json:"url,omitempty"`
	Error  string `json:"error,omitempty"`
}

type callbackTaskResult struct {
	ImageURL        string `json:"img_url"`
	ErrorMsg        string `json:"error_msg"`
	DisplayErrorMsg string `json:"display_error_msg"`
}
