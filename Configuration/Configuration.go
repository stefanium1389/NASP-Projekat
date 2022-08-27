package Configuration

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct{
	HllP uint `yaml:"hll_p"`
	CacheCapacity int `yaml:"cache_capacity"`
	TokenBucketResetInterval int64 `yaml:"token_bucket_reset_interval"`
	TokenBucketMaxTokenNum int `yaml:"token_bucket_max_token_num"`
	CmsEpsilon float64 `yaml:"cms_epsilon"`
	CmsDelta float64 `yaml:"cms_delta"`
}

func (config *Config) Load(){
	data, err := ioutil.ReadFile("./Configuration/Config.yaml")
	if err != nil {
		config.HllP = 4
		config.CacheCapacity = 10
		config.TokenBucketResetInterval = 10
		config.TokenBucketMaxTokenNum = 10
		config.CmsEpsilon = 0.1
		config.CmsDelta = 0.1
	}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		panic(err)
	}

}
