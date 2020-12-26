package config

import (
	"log"

	viper "github.com/spf13/viper"
)

// Configurations exported
type Configurations struct {
	Server   ServerConfigurations
	Services ServiceConfigurations
}

// ServerConfigurations exported
type ServerConfigurations struct {
	Port string
}

// ServiceConfigurations exported
type ServiceConfigurations struct {
	UploadService string
}

// GetConfigs Get Configurations from config.yaml and set in Configurations struct
func GetConfigs() (Configurations, error) {
	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yaml")   // type
	viper.AddConfigPath("/usr/local/bin/") // optionally look for config in the working directory
	viper.AddConfigPath(".") // optionally look for config in the working directory
	viper.AutomaticEnv()          // enable viper to read env

	// store in configuration struct
	var configs Configurations

	if err := viper.ReadInConfig(); err != nil {
		log.Println(err)
		return configs, err
	}
	if err := viper.Unmarshal(&configs); err != nil {
		log.Println(err)
		return configs, err
	}
	return configs, nil
}