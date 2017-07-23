package main

import (
	"strings"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/docker/docker/daemon/logger"
	zmq "github.com/pebbe/zmq4"

	"fmt"
)

//定义一个struct,这里包含一个zmq套接字
type ZmqLogger struct {
	writer      *zmq.Socket
	containerId string
	tenantId    string
	serviceId   string
	felock      sync.Mutex
}

// Name is the name of the file that the jsonlogger logs to.
const name = "zmq_logger"
const zmqAddress = "zmqAddress"

//定义init方法调用logger注册器的方法注册当前driver
//和参数验证方法。
func init() {
	if err := logger.RegisterLogDriver(name, New); err != nil {
		logrus.Fatal(err)
	}
	if err := logger.RegisterLogOptValidator(name, ValidateLogOpt); err != nil {
		logrus.Fatal(err)
	}
}

//实现一个上文提到的Creator方法注册logdriver.
//这里新建一个zmq套接字构建一个实例
func New(info logger.Info) (logger.Logger, error) {
	zmqaddress := info.Config[zmqAddress]

	puber, err := zmq.NewSocket(zmq.PUB)
	if err != nil {
		return nil, err
	}
	var (
		env       = make(map[string]string)
		tenantId  string
		serviceId string
	)
	for _, pair := range info.ContainerEnv {
		p := strings.SplitN(pair, "=", 2)
		//logrus.Errorf("ContainerEnv pair: %s", pair)
		if len(p) == 2 {
			key := p[0]
			value := p[1]
			env[key] = value
		}
	}
	tenantId = env["TENANT_ID"]
	serviceId = env["SERVICE_ID"]

	if tenantId == "" {
		tenantId = "default"
	}

	if serviceId == "" {
		serviceId = "default"
	}

	puber.Connect(zmqaddress)

	return &ZmqLogger{
		writer:      puber,
		containerId: info.ID(),
		tenantId:    tenantId,
		serviceId:   serviceId,
		felock:      sync.Mutex{},
	}, nil
}

//实现Log方法，这里使用zmq socket发送日志消息
//这里必须注意，zmq socket是线程不安全的，我们知道
//本方法可能被两个线程(复制stdout和肤质stderr)调用//必须使用锁保证线程安全。否则会发生错误。
func (s *ZmqLogger) Log(msg *logger.Message) error {
	s.felock.Lock()
	defer s.felock.Unlock()
	s.writer.Send(s.tenantId, zmq.SNDMORE)
	s.writer.Send(s.serviceId, zmq.SNDMORE)
	if msg.Source == "stderr" {
		s.writer.Send(s.containerId+": "+string(msg.Line), zmq.DONTWAIT)
	} else {
		s.writer.Send(s.containerId+": "+string(msg.Line), zmq.DONTWAIT)
	}
	return nil
}

//实现Close方法，这里用来关闭zmq socket。
//同样注意线程安全，调用此方法的是容器关闭协程。
func (s *ZmqLogger) Close() error {
	s.felock.Lock()
	defer s.felock.Unlock()
	if s.writer != nil {
		return s.writer.Close()
	}
	return nil
}

func (s *ZmqLogger) Name() string {
	return name
}

//验证参数的方法，我们使用参数传入zmq pub的地址。
func ValidateLogOpt(cfg map[string]string) error {
	for key := range cfg {
		switch key {
		case zmqAddress:
		default:
			return fmt.Errorf("unknown log opt '%s' for %s log driver", key, name)
		}
	}
	if cfg[zmqAddress] == "" {
		return fmt.Errorf("must specify a value for log opt '%s'", zmqAddress)
	}
	return nil
}
