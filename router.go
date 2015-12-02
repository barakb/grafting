package go_rafting

type RouteTarget interface {
	Address() string
	OutboundChan() chan Message
	InboundChan() chan Message
}

type Router struct {
	targets map[string]RouteTarget
	done    chan struct{}
}

func (router Router) Register(target RouteTarget) {
	router.targets[target.Address()] = target
	go router.serveOutbound(target)
}

func (router Router) ShutDown() {
	close(router.done)
}

func NewRouter() Router {
	return Router{make(map[string]RouteTarget), make(chan struct{})}
}

func (router Router) serveOutbound(target RouteTarget) {
	for {
		select {
		case message := <-target.OutboundChan():
			router.dispatch(message)
		case <-router.done:
			return
		}
	}
}

func (router Router) dispatch(message Message) {
	if target, ok := router.targets[message.To()]; ok {
		target.InboundChan() <- message
	}
}
