package lognats

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io/ioutil"
	"strings"

	"github.com/devopsfaith/krakend/config"
	"github.com/devopsfaith/krakend/logging"
	"github.com/gin-gonic/gin"
	stan "github.com/nats-io/stan.go"
)

const authHeader = "Authorization"
const forwardHeader = "X-Forwarded-For"
const Namespace = "github_com/zean00/lognats"

//NatsConfig log nats config
type NatsConfig struct {
	NatsURL     string
	ClusterName string
	ClientName  string
	EventName   string
	LogPayload  bool
	LogHeader   bool
	nclient     stan.Conn
	logger      logging.Logger
}

//Payload message payload
type Payload struct {
	Method      string      `json:"method,omitempty" mapstructure:"method"`
	Path        string      `json:"path,omitempty" mapstructure:"path"`
	URL         string      `json:"url,omitempty" mapstructure:"url"`
	Data        interface{} `json:"data,omitempty" mapstructure:"data"`
	Headers     interface{} `json:"headers,omitempty" mapstructure:"headers"`
	Requestor   interface{} `json:"requestor,omitempty" mapstructure:"requestor"`
	StatusCode  int         `json:"status_code,omitempty" mapstructure:"status_code"`
	IPAddress   string      `json:"ip_address,omitempty" mapstructure:"ip_address"`
	ForwardedIP string      `json:"ip_forwarded,omitempty" mapstructure:"ip_forwarded"`
}

//New create middleware
func New(logger logging.Logger, config config.ExtraConfig) gin.HandlerFunc {
	cfg := configGetter(logger, config)
	if cfg == nil {
		logger.Info("[lognats] Empty config")
		return func(c *gin.Context) {
			c.Next()
		}
	}
	return func(c *gin.Context) {

		payload := Payload{
			Method:    c.Request.Method,
			URL:       c.Request.URL.RequestURI(),
			Path:      c.Request.URL.Path,
			IPAddress: c.Request.RemoteAddr,
		}

		if fw := c.Request.Header.Get(forwardHeader); fw != "" {
			payload.ForwardedIP = fw
		}

		if token := c.Request.Header.Get(authHeader); token != "" {
			token = strings.TrimPrefix(token, "Bearer ")
			//Just in case using lower case bearer
			token = strings.TrimPrefix(token, "bearer ")

			parts := strings.Split(token, ".")
			if len(parts) == 3 {
				b, err := base64.RawStdEncoding.DecodeString(parts[1])
				var claim map[string]interface{}
				if err == nil {
					if err := json.Unmarshal(b, &claim); err == nil {
						payload.Requestor = claim
					}
				}
			}
		}

		if cfg.LogPayload {
			raw, _ := ioutil.ReadAll(c.Request.Body)
			//log.Println("RAW body ", raw)
			if len(raw) > 0 {
				c.Request.Body = ioutil.NopCloser(bytes.NewReader(raw))
				var pl map[string]interface{}
				if err := json.Unmarshal(raw, &pl); err == nil {
					payload.Data = pl
				}
			}
		}

		if cfg.LogHeader {
			payload.Headers = c.Request.Header
		}

		c.Next()

		payload.StatusCode = c.Writer.Status()

		cfg.publish(payload)
	}
}

func configGetter(logger logging.Logger, config config.ExtraConfig) *NatsConfig {
	v, ok := config[Namespace]
	if !ok {
		return nil
	}
	tmp, ok := v.(map[string]interface{})
	if !ok {
		return nil
	}

	cfg := new(NatsConfig)

	url, ok := tmp["nats_url"].(string)
	if !ok {
		return nil
	}
	cfg.NatsURL = url

	cluster, ok := tmp["nats_cluster"].(string)
	if !ok {
		return nil
	}

	cfg.ClusterName = cluster

	name, ok := tmp["client_name"].(string)
	if !ok {
		name = "krakend"
	}
	cfg.ClientName = name

	event, ok := tmp["event_name"].(string)
	if !ok {
		event = "apigw.request"
	}
	cfg.EventName = event

	lp, ok := tmp["log_payload"].(bool)
	if ok {
		cfg.LogPayload = lp
	}

	lh, ok := tmp["log_header"].(bool)
	if ok {
		cfg.LogHeader = lh
	}

	conn, err := stan.Connect(cfg.ClusterName, cfg.ClusterName, stan.NatsURL(cfg.NatsURL))
	if err != nil {
		logger.Error("[lognats] Error connecting to nats server ")
		logger.Error(err)
		return nil
	}

	cfg.logger = logger
	cfg.nclient = conn

	return cfg
}

func (l *NatsConfig) publish(data interface{}) error {
	b, _ := json.Marshal(data)
	if err := l.nclient.Publish(l.EventName, b); err != nil {
		l.logger.Error("[lognats] Error publishing event ", err)
		return err
	}
	return nil
}
