package db

import (
	"errors"
	"gokit/config"
	"gokit/log"
	"time"

	"github.com/vuleetu/pools"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
)

var mysqlPools = map[string]*pools.RoundRobin{}

func StartMysql() {
	var mysqlSpecs map[interface{}]interface{}
	err := config.Get("mysql", &mysqlSpecs)
	if err != nil {
		log.Fatalln(err)
	}

	log.Debug(mysqlSpecs)
	for name, spec := range mysqlSpecs {
		newMysqlPool(name.(string), spec.(map[interface{}]interface{}))
	}
}

func GetMySQL(groupName, role string) (*MysqlResource, error) {
	masterPool, ok := mysqlPools[groupName+"#"+role]
	if !ok {
		log.Error("Pool not found, group name: ", groupName, ", role: ", role, " , type: mysql")
		return nil, POOL_NOT_FOUND
	}

	log.Debug("Group name: ", groupName, ", role: ", role, ", ", masterPool.StatsJSON(), ", type: mysql")
	i := 1
	for i < 5 {
		r, err := masterPool.Get()
		if err != nil {
			log.Error("Get resource from pool failed, ", err, ", group name: ", groupName, ", role: ", role, ", type: mysql")
			return nil, err
		}

		mr, ok := r.(*MysqlResource)
		if !ok {
			log.Error("Convert resource to mysql session failed, , group name: ", groupName, ", role: ", role, ", type: mysql")
			return nil, TYPE_CONVERSION_FAILED
		}

		log.Debug("Check if mysql connection is alive, , group name: ", groupName, ", role: ", role)
		if err = mr.Db().Ping(); err != nil {
			log.Warn("Ping failed", err, ", group name: ", groupName, ", role: ", role)
			log.Warn("Try reconect, , group name: ", groupName, ", role: ", role)
			mr.Release()
			i++
			continue
		}

		log.Debug("Ping success, , group name: ", groupName, ", role: ", role)
		log.Debug("Mysql connection is alive now, , group name: ", groupName, ", role: ", role)
		log.Debug("Got resource, , group name: ", groupName, ", role: ", role)
		return mr, nil
	}

	log.Error("Reconect reached maximum times: 5, , group name: ", groupName, ", role: ", role)
	return nil, errors.New("Can not establish connection to mysql")
}

type MysqlResource struct {
	GroupName string
	Role      string
	db        mysql.Conn
	spec      *MysqlSpec
}

func (r *MysqlResource) ToSQLError(err error) *mysql.Error {
	val, _ := err.(*mysql.Error)
	return val
}

func (r *MysqlResource) Db() mysql.Conn {
	return r.db
}

func (r *MysqlResource) Close() {
	r.db.Close()
}

func (r *MysqlResource) IsClosed() bool {
	return r.db.Ping() != nil
}

func (r *MysqlResource) Release() {
	log.Debug("Release mysql resource, group name: ", r.GroupName, ", role: ", r.Role)
	mysqlPools[r.GroupName+"#"+r.Role].Put(r)
}

func newMysqlFactory(groupName, role string, spec *MysqlSpec) pools.Factory {
	return func() (pools.Resource, error) {
		conn := mysql.New("tcp", "", spec.Addr, spec.User, spec.Passwd, spec.DBName)
		//conn.Debug = spec.Debug

		err := conn.Connect()
		if err != nil {
			log.Error("Connect to mysql failed", err, ", info", spec)
			return nil, err
		}

		log.Debug("Connect to mysql success, for", spec)
		log.Debug("Set names to utf8mb4")
		conn.Register("SET NAMES 'utf8mb4'")

		return &MysqlResource{groupName, role, conn, spec}, nil
	}
}

func newMysqlPool(groupName string, rawSpec YAML_MAP) {
	log.Debug("Group name: ", groupName, ", Spec: ", rawSpec)
	var group MysqlGroup
	err := unmarshal(rawSpec, &group, false)
	if err != nil {
		log.Fatalln(err)
	}

	if group.Master == nil {
		log.Fatalln("Mysql database group: ", groupName, " not specified")
	}

	newMysqlPool1(groupName, "master", group.Master)

	if group.Slave != nil {
		newMysqlPool1(groupName, "slave", group.Slave)
	}
}

func newMysqlPool1(groupName, name string, spec *MysqlSpec) {
	if spec.Addr == "" {
		spec.Addr = "localhost"
	}

	if spec.Pool < 1 {
		spec.Pool = DEFAULT_MYSQL_POOL_SIZE
	}

	log.Debug("Setting for mysql, group: ", groupName, ", name:", name, ", spec:", spec)
	log.Debug("Trying to connect mysql, group: ", groupName, ", name: ", name, ", spec:", spec)
	conn := mysql.New("tcp", "", spec.Addr, spec.User, spec.Passwd, spec.DBName)
	//conn.Debug = spec.Debug

	err := conn.Connect()
	if err != nil {
		log.Fatalln("Failed to connect to mysql, group: ", groupName, ", name:", name, ", spec:", spec, ", err:", err)
	} else {
		log.Debug("Connected mysql, group: ", groupName, ", name: ", name, ", spec:", spec)
	}
	defer conn.Close()

	p := pools.NewRoundRobin(spec.Pool, time.Minute*10)
	p.Open(newMysqlFactory(groupName, name, spec))
	mysqlPools[groupName+"#"+name] = p
}
