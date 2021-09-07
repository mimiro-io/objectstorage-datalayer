package entity

type Entity struct {
	ID         string                 `json:"id"`
	IsDeleted  bool                   `json:"deleted"`
	References map[string]interface{} `json:"refs"`
	Properties map[string]interface{} `json:"props"`
	Recorded   string                 `json:"recorded,omitempty"`
}

// NewEntity Create a new entity with global uuid and internal resource id
func NewEntity() *Entity {
	e := Entity{}
	e.Properties = make(map[string]interface{})
	e.References = make(map[string]interface{})
	return &e
}
