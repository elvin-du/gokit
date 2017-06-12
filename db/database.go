package db

import (
	"encoding/json"
	"gokit/log"

	_ "github.com/ziutek/mymysql/native"
)

func StartAll() {
	StartMysql()
	StartRedis()
}

func unmarshal(data YAML_MAP, target interface{}, onelevel bool) error {
	if onelevel {
		mp := map[string]interface{}{}
		for k, v := range data {
			mp[k.(string)] = v
		}

		str, err := json.Marshal(mp)
		if err != nil {
			log.Debug(err)
			return err
		}

		return json.Unmarshal(str, target)
	}

	var m = map[string]map[string]interface{}{}
	for k, v := range data {
		mp := map[string]interface{}{}
		for k1, v1 := range v.(map[interface{}]interface{}) {
			mp[k1.(string)] = v1
		}

		m[k.(string)] = mp
	}

	str, err := json.Marshal(m)
	if err != nil {
		log.Debug(err)
		return err
	}

	return json.Unmarshal(str, target)
}
