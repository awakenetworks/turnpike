package turnpike

import (
	"github.com/nats-io/nats"
	"sync"
)

// Replacement Broker Implementation using NATS
type NatsBroker struct {
	C             *nats.EncodedConn
	Subscriptions map[ID]*nats.Subscription
	m             sync.Mutex
}

func NewNatsBroker(url string) (Broker, error) {
	if url == "" {
		url = nats.DefaultURL
	}
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, err
	}
	c, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	return &NatsBroker{C: c, Subscriptions: make(map[ID]*nats.Subscription)}, nil
}

func (nb *NatsBroker) Publish(pub Sender, msg *Publish) {
	pubID := NewID()
	evtTemplate := Event{
		Publication: pubID,
		Arguments:   msg.Arguments,
		ArgumentsKw: msg.ArgumentsKw,
		Details:     make(map[string]interface{}),
	}
	nb.C.Publish(string(msg.Topic), evtTemplate)
	if doPub, _ := msg.Options["acknowledge"].(bool); doPub {
		pub.Send(&Published{Request: msg.Request, Publication: pubID})
	}
}

func (nb *NatsBroker) Subscribe(sub Sender, msg *Subscribe) {
	id := NewID()
	natsTopic, err := nb.C.Subscribe(string(msg.Topic), func(e *Event) {
		event := *e
		event.Subscription = id
		if e.Subscription == e.Publication {
			// don't send event to ourselves
			return
		}
		err := sub.Send(&event)
		if err != nil {
			log.Printf("Error sending to subscriber %d %v", id, err)
			nb.Unsubscribe(sub, &Unsubscribe{NewID(), id})
		}
	})
	if err != nil {
		log.Printf("unable to add subscription %q %v", msg.Topic, err)
	}
	// lock write
	nb.m.Lock()
	defer nb.m.Unlock()
	nb.Subscriptions[id] = natsTopic
	sub.Send(&Subscribed{Request: msg.Request, Subscription: id})
}

func (nb *NatsBroker) Unsubscribe(sub Sender, msg *Unsubscribe) {
	// Lock because we are reading and changing
	nb.m.Lock()
	defer nb.m.Unlock()
	subscription, ok := nb.Subscriptions[msg.Subscription]
	if !ok {
		err := &Error{
			Type:    msg.MessageType(),
			Request: msg.Request,
			Error:   ErrNoSuchSubscription,
		}
		sub.Send(err)
		log.Printf("Error unsubscribing: no such subscription %v", msg.Subscription)
		return
	}
	delete(nb.Subscriptions, msg.Subscription)
	subscription.Unsubscribe()
	sub.Send(&Unsubscribed{Request: msg.Request})
}
