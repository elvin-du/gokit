package subscriber

import (
	"gokit/log"
	"gokit/util"
	"time"

	"github.com/streadway/amqp"
)

type Subscriber struct {
	addr, exchange, route_key, queue string
	pool                             int
	auto_delete, auto_ack            bool
	wait_time                        time.Duration
	callback                         map[string]func(string, string)
}

func NewSubscriber(addr, exchange, route_key, queue string, pool int, auto_delete, auto_ack bool, wait_time time.Duration) *Subscriber {
	return &Subscriber{addr, exchange, route_key, queue, pool, auto_delete, auto_ack, wait_time, nil}
}

func (sub *Subscriber) Start() {
	if sub.queue == "" {
		var err error
		sub.queue, err = util.RandomString(10)
		if err != nil {
			log.Fatalln(err)
		}
	}

	log.Debug("Queue name: ", sub.queue)
	for i := 0; i < sub.pool; i++ {
		log.Infoln("Starting subscribe client, #", i)
		go sub.startWorker(0)
	}
}

func (sub *Subscriber) RegisterEventHandler(event string, callback func(string, string)) {
	if sub.callback == nil {
		sub.callback = make(map[string]func(string, string))
	}

	sub.callback[event] = callback
}

func (sub *Subscriber) startWorker(lastWait time.Duration) {
	conn, err := amqp.Dial(sub.addr)
	if err != nil {
		log.Errorln(err)
		sub.restartWorker(lastWait + sub.wait_time)
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Errorln(err)
		conn.Close()
		sub.restartWorker(lastWait + sub.wait_time)
		return
	}

	err = ch.ExchangeDeclare(sub.exchange, "topic", true, false, false, false, nil)
	if err != nil {
		log.Errorln(err)
		conn.Close()
		sub.restartWorker(lastWait + sub.wait_time)
		return
	}

	_, err = ch.QueueDeclare(sub.queue, true, sub.auto_delete, false, false, nil)
	if err != nil {
		log.Errorln(err)
		conn.Close()
		sub.restartWorker(lastWait + sub.wait_time)
		return
	}

	err = ch.QueueBind(sub.queue, sub.route_key, sub.exchange, false, nil)
	if err != nil {
		log.Errorln(err)
		conn.Close()
		sub.restartWorker(lastWait + sub.wait_time)
		return
	}

	ch.Qos(2, 0, false)

	notifies, err := ch.Consume(sub.queue, "some_consumer", sub.auto_ack, false, true, false, nil)
	if err != nil {
		log.Errorln(err)
		conn.Close()
		sub.restartWorker(lastWait + sub.wait_time)
		return
	}

	log.Infoln("Subscribe worker started success")
	log.Infoln("Waiting for message")
	for notify := range notifies {
		log.Infoln("Message arrived, body is", string(notify.Body), "routing key is", notify.RoutingKey)
		if callback, ok := sub.callback[notify.RoutingKey]; ok {
			callback(notify.RoutingKey, string(notify.Body))
		} else {
			log.Warnln("No registered callback available for", notify.RoutingKey)
		}

		if !sub.auto_ack {
			notify.Ack(false)
		}
	}

	log.Errorln("Channel closed, restart worker now")
	conn.Close()
	sub.restartWorker(0)
}

func (sub *Subscriber) restartWorker(wait time.Duration) {
	if wait > 0 {
		log.Infoln("Wait for", wait, "to restart worker")
		time.Sleep(wait)
	}

	log.Infoln("Restarting worker now")
	sub.startWorker(wait)
}
