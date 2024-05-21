package actionDispatcher

func NewServiceManager(mediaType string, actions []string, host string, port uint32) (*ServiceManager, error) {
	return &ServiceManager{
		mediaType: mediaType,
		actions:   actions,
		host:      host,
		port:      port,
	}, nil
}

type ServiceManager struct {
	mediaType string
	actions   []string
	host      string
	port      uint32
}
