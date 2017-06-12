package jpush

import (
	"encoding/json"
	"errors"
	"gokit/config"
	"gokit/log"
	"sync"
	"time"

	"github.com/elvin-du/jpush-api-go-client"
	"github.com/elvin-du/jpush-api-go-client/push"
)

var (
	_gAppKey        string
	_gMasterSecret  string
	_gIOSProduction bool
	_gPushDebug     bool = false
	_gPushLimit     int  = 600 // op/min
)

var (
	E_PUSH_FAILED = errors.New("push failed")
)

func loadPushConf() {
	err := config.Get("push:jpush:app_key", &_gAppKey)
	if err != nil {
		log.Fatalln(err)
	}

	err = config.Get("push:jpush:master_secret", &_gMasterSecret)
	if err != nil {
		log.Fatalln(err)
	}

	err = config.Get("push:jpush:ios_production", &_gIOSProduction)
	if err != nil {
		log.Fatalln(err)
	}

	err = config.Get("push:jpush:debug", &_gPushDebug)
	if err != nil {
		log.Fatalln(err)
	}

	err = config.Get("push:jpush:limit", &_gPushLimit)
	if err != nil {
		log.Fatalln(err)
	}
}

func JPushAll(alert, sound string, badge int, extras map[string]interface{}) error {
	audience := push.NewAudience()
	audience.All()
	return jPush(_gAppKey, _gMasterSecret, alert, sound, audience, badge, _gIOSProduction, _gPushDebug, extras)
}

func JPushOne(alert, sound string, regId []string, badge int, extras map[string]interface{}) error {
	audience := push.NewAudience()
	audience.SetRegistrationId(regId)
	return jPush(_gAppKey, _gMasterSecret, alert, sound, audience, badge, _gIOSProduction, _gPushDebug, extras)
}

var start = time.Now()
var count int
var mutex sync.Mutex
var maxPush = make(chan struct{}, _gPushLimit)

func jPush(appKey, masterSecret, alert, sound string, audience *push.Audience, badge int, iosProduction, debug bool, extras map[string]interface{}) error {
	//限速
	maxPush <- struct{}{}
	defer func() {
		<-maxPush
	}()

	for {
		mutex.Lock()

		if count == _gPushLimit-1 {
			duration := time.Since(start)
			if duration < time.Minute*1 {
				mutex.Unlock()
				log.Warn("sleep start...")
				log.Warn("sleep_start_time=", start)
				time.Sleep(time.Minute*1 - duration + time.Minute*1)
				log.Warn("sleep end")
				log.Warn("sleep_end_time=", start)
				continue
			}

			start = time.Now()
			count = 0
		}

		mutex.Unlock()
		break
	}

	count++

	// platform 对象
	platform := push.NewPlatform()
	// 用 Add() 方法添加具体平台参数，可选: "all", "ios", "android"
	platform.Add("ios", "android")
	// 或者用 All() 方法设置所有平台
	// platform.All()

	//	// audience 对象，表示消息受众
	//	audience := push.NewAudience()
	//	audience.SetRegistrationId(regId)
	//	// 和 platform 一样，可以调用 All() 方法设置所有受众
	//	// audience.All()

	// notification 对象，表示 通知，传递 alert 属性初始化
	notification := push.NewNotification(alert)

	// android 平台专有的 notification，用 alert 属性初始化
	androidNotification := push.NewAndroidNotification(alert)
	//	androidNotification.Title = "title"
	for k, v := range extras {
		androidNotification.AddExtra(k, v)
	}

	notification.Android = androidNotification

	// iOS 平台专有的 notification，用 alert 属性初始化
	iosNotification := push.NewIosNotification(alert)
	iosNotification.Badge = badge
	if "" != sound {
		iosNotification.Sound = sound
	}
	for k, v := range extras {
		iosNotification.AddExtra(k, v)
	}

	// Validate 方法可以验证 iOS notification 是否合法
	// 一般情况下，开发者不需要直接调用此方法，这个方法会在构造 PushObject 时自动调用
	// iosNotification.Validate()

	notification.Ios = iosNotification

	// option 对象，表示推送可选项
	options := push.NewOptions()
	// iOS 平台，是否推送生产环境，false 表示开发环境；如果不指定，就是生产环境
	options.ApnsProduction = iosProduction
	options.TimeToLive = 86400 //离线消息保留时长（单位：秒）一天
	message := push.NewMessage("useless msg")
	message.Title = ""

	payload := push.NewPushObject()
	payload.Platform = platform
	payload.Audience = audience
	payload.Notification = notification
	payload.Message = message

	payload.Options = options

	data, err := json.Marshal(payload)
	if err != nil {
		log.Errorln("json.Marshal PushObject failed:", err)
		return err
	}
	log.Infoln("payload:", string(data))

	// 创建 JPush 的客户端
	jclient := jpush.NewJPushClient(appKey, masterSecret)
	jclient.SetDebug(debug)

	// Push 会推送到客户端
	result, err := jclient.Push(payload)
	if err != nil {
		log.Errorln("PushValidate failed:", err)
		return err
	}

	if result.Ok() {
		log.Infoln("PushValidate result:", result)
	} else {
		log.Errorln(result.Error)
		log.Errorln("PushValidate result:", result)
		return E_PUSH_FAILED
	}

	return nil
	// PushValidate 的参数和 Push 完全一致
	// 区别在于，PushValidate 只会验证推送调用成功，不会向用户发送任何消息
	//	result, err := jclient.PushValidate(payload)
	//	if err != nil {
	//		log.Print("\nPushValidate failed:", err)
	//	} else {
	//		log.Println("\nPushValidate result:", result)
	//	}
}
