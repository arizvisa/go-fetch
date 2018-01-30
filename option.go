package fetch

type Configurable interface {
	Config(key string, value interface{}) error
}
