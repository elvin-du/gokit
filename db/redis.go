package db

import (
	"errors"
	"gokit/config"
	"gokit/log"
	"time"

	"github.com/vuleetu/pools"
	_ "github.com/ziutek/mymysql/native"
	"gopkg.in/redis.v2"
)

const MAX_TRIED = 3

var redisPools = map[string]*pools.RoundRobin{}

func StartRedis() {
	var redisSpecs map[interface{}]interface{}
	err := config.Get("redis", &redisSpecs)
	if err != nil {
		log.Fatalln(err)
	}

	log.Debug(redisSpecs)
	for name, spec := range redisSpecs {
		newRedisPool(name.(string), spec.(map[interface{}]interface{}))
	}
}

func GetMRedis() (*RedisResource, error) {
	return GetRedis("master")
}

func GetRedis(name string) (*RedisResource, error) {
	//hard code to master
	masterPool, ok := redisPools[name]
	if !ok {
		log.Error("Pool not found: #", name, ", type: redis")
		return nil, POOL_NOT_FOUND
	}

	log.Debug("#", name, "#", masterPool.StatsJSON(), ", type: redis")
	r, err := masterPool.Get()
	if err != nil {
		log.Error("Get resource from pool failed", err, "#", name, ", type: redis")
		return nil, err
	}

	mr, ok := r.(*RedisResource)
	if !ok {
		log.Error("Convert resource to redis session failed, #", name, ", type: redis")
		return nil, TYPE_CONVERSION_FAILED
	}

	log.Debug("Check if redis connection is alive, #", name)
	var i = 0
	for {
		if i >= MAX_TRIED {
			log.Error("Reconect reached maximum times:", i, "#", name)
			return nil, errors.New("Can not establish connection to redis")
		}

		if err = mr.client.Ping().Err(); err != nil {
			log.Warn("Ping failed", err, "#", name)
			log.Warn("Try reconect, #", name)

			var opt redis.Options
			opt.Addr = mr.spec.Addr
			opt.DB = int64(mr.spec.Db)
			opt.PoolSize = 1

			client := redis.NewTCPClient(&opt)
			err = client.Ping().Err()
			if err != nil {
				log.Error("Connect to redis failed", err, ", info", mr.spec)
				i++
				continue
			}

			mr = &RedisResource{name, client, mr.spec}
			break
		}

		log.Debug("Ping success, #", name)
		break
	}

	log.Debug("Redis connection is alive now, #", name)
	log.Debug("Got resource, #", name)
	return mr, nil
}

type RedisResource struct {
	kind   string
	client *redis.Client
	spec   *RedisSpec
}

func (r *RedisResource) Nil() error {
	return redis.Nil
}

func (r *RedisResource) Client() *redis.Client {
	return r.client
}

func (r *RedisResource) Close() {
	r.client.Close()
}

func (r *RedisResource) IsClosed() bool {
	return r.client.Ping().Err() != nil
}

func (r *RedisResource) Release() {
	log.Debug("Release redis resource, group name: ", r.kind)
	redisPools[r.kind].Put(r)
}

func newRedisPool(name string, rawSpec YAML_MAP) {
	var spec RedisSpec
	err := unmarshal(rawSpec, &spec, true)
	if err != nil {
		log.Fatalln(err)
	}

	if spec.Addr == "" {
		log.Fatalln("Addr not set for", name)
	}

	if spec.Pool < 1 {
		spec.Pool = DEFAULT_REDIS_POOL_SIZE
	}

	if spec.Db < 0 {
		spec.Db = 0
	}

	log.Debug("Setting for redis, name:", name, ", spec:", spec)
	var opt redis.Options
	opt.Addr = spec.Addr
	opt.DB = int64(spec.Db)
	opt.PoolSize = 1

	log.Debug("Trying to connect to redis, name: ", name, ", spec:", spec)
	client := redis.NewTCPClient(&opt)
	err = client.Ping().Err()
	if err != nil {
		log.Fatalln("Failed to connect to redis, name:", name, ", spec:", spec, ", err:", err)
	} else {
		log.Debug("Connected to redis, name: ", name, ", spec:", spec)
	}
	defer client.Close()

	p := pools.NewRoundRobin(spec.Pool, time.Minute*10)
	p.Open(newRedisFactory(name, &spec))
	redisPools[name] = p
}

func newRedisFactory(kind string, spec *RedisSpec) pools.Factory {
	return func() (pools.Resource, error) {
		var opt redis.Options
		opt.Addr = spec.Addr
		opt.DB = int64(spec.Db)
		//opt.PoolSize = 1

		client := redis.NewTCPClient(&opt)
		err := client.Ping().Err()
		if err != nil {
			log.Error("Connect to redis failed", err, ", info", spec)
			return nil, err
		}

		return &RedisResource{kind, client, spec}, nil
	}
}
