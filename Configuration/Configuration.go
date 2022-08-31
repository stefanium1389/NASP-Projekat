package Configuration

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type Config struct {
	HllP                     int     `yaml:"hll_p"`
	CacheCapacity            int     `yaml:"cache_capacity"`
	TokenBucketResetInterval int64   `yaml:"token_bucket_reset_interval"`
	TokenBucketMaxTokenNum   int     `yaml:"token_bucket_max_token_num"`
	CmsEpsilon               float64 `yaml:"cms_epsilon"`
	CmsDelta                 float64 `yaml:"cms_delta"`
	MemtableThreshold        int     `yaml:"memtable_threshold"`
	SLMaxLevel               int     `yaml:"sl_max_level"`
	SLProbability            float32 `yaml:"sl_probability"`
	WALSegment               uint32  `yaml:"wal_segment"`
	WALLowMark               uint32  `yaml:"wal_low_mark"`
	FPRateBloomFilter        float64 `yaml:"false_positive_rate"`
}

func Load() *Config {
	data, err := ioutil.ReadFile("./Configuration/Config.yaml")
	config := Config{}
	if err != nil {
		config.HllP = 4
		config.CacheCapacity = 10
		config.TokenBucketResetInterval = 10
		config.TokenBucketMaxTokenNum = 10
		config.CmsEpsilon = 0.1
		config.CmsDelta = 0.1
		config.MemtableThreshold = 7
		config.SLMaxLevel = 15
		config.SLProbability = 0.5
		config.WALSegment = 5
		config.WALLowMark = 3
		config.FPRateBloomFilter = 0.4
	}
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		panic(err)
	}
	return &config
}
