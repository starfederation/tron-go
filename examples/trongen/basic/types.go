package basic

type Address struct {
	Line1 string `json:"line1"`
	City  string `json:"city"`
	State string `json:"state"`
	Zip   string `json:"zip"`
}

type User struct {
	ID      string   `json:"id"`
	Name    string   `json:"name"`
	Email   string   `json:"email,omitempty"`
	Address Address  `json:"address"`
	Tags    []string `json:"tags,omitempty"`
}
