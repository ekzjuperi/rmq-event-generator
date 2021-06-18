package models

// Event struct contains structure of event.
type Event struct {
	TimeStamp int64 `json:"ts"`
	ProfileID int64 `json:"pid"`
	ActionID  int   `json:"aid"`
}
