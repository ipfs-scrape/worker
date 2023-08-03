package backend

type Backend interface {
	Create(item any) error
	Read(id string) (any, error)
	Update(item any) error
	Delete(id string) error
	Scan(prefix string) ([]any, error)
}
