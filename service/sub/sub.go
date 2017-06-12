package sub

import (
	"gokit/config"
	"gokit/log"
	"gokit/rabbitmq/subscriber"
	"time"
)

var (
	WAIT_STEP  = time.Second * 10
	autoDelete = false
	autoACK    = true

	sub *subscriber.Subscriber
)

func InitSubscriber(subName string) {
	var addr string
	err := config.Get(subName+":addr", &addr)
	if err != nil {
		log.Fatalln(err)
	}

	var exchange string
	err = config.Get(subName+":exchange", &exchange)
	if err != nil {
		log.Fatalln(err)
	}

	var route_key string
	err = config.Get(subName+":route_key", &route_key)
	if err != nil {
		log.Fatalln(err)
	}

	var pool int
	err = config.Get(subName+":pool", &pool)
	if err != nil {
		log.Fatalln(err)
	}

	var wait_time int
	err = config.Get(subName+":reconnect", &wait_time)
	if err != nil {
		log.Fatalln(err)
	}

	if wait_time > 0 {
		WAIT_STEP = time.Duration(wait_time) * time.Second
	}

	var queue string
	err = config.Get(subName+":queue", &queue)
	if err != nil {
		log.Debugln(err)
	}

	autoDel := false
	err = config.Get(subName+":auto_delete", &autoDel)
	if err == nil {
		autoDelete = autoDel
	}

	autoAck := false
	err = config.Get(subName+":auto_ack", &autoAck)
	if err == nil {
		autoACK = autoAck
	} else {
		log.Warnf("retrieve %s:auto_ack from config.yml failed,err:%s", subName, err.Error())
	}

	sub = subscriber.NewSubscriber(addr, exchange, route_key, queue, pool, autoDelete, autoACK, WAIT_STEP)
}

func RegisterEventHandler(event string, callback func(string, string)) {
	sub.RegisterEventHandler(event, callback)
}

func Run() {
	sub.Start()
}
