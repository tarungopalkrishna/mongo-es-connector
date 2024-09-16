package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/knadh/koanf/parsers/json"
	"github.com/knadh/koanf/parsers/yaml"
	"github.com/knadh/koanf/providers/file"
	"github.com/knadh/koanf/providers/posflag"
	"github.com/knadh/koanf/v2"
	"github.com/rs/zerolog/log"
	flag "github.com/spf13/pflag"
)

func initFlags(ko *koanf.Koanf) {
	f := flag.NewFlagSet("config", flag.ContinueOnError)
	f.Usage = func() {
		fmt.Println(f.FlagUsages())
		os.Exit(0)
	}

	f.StringSlice("config", []string{"config.yaml"}, "path to one or more config files (will be merged in order)")
	f.Bool("version", false, "show current version of the build")

	if err := f.Parse(os.Args[1:]); err != nil {
		log.Fatal().Msgf("error loading flags: %v", err)
	} else {
		log.Trace().Msg("No errors when parsing the flags")
	}

	if err := ko.Load(posflag.Provider(f, ".", ko), nil); err != nil {
		log.Fatal().Msgf("error reading flag config: %v", err)
	}
}

func initConfig(ko *koanf.Koanf) error {

	log.Info().Msg("Loading configs")
	// Load one or more config files. Keys in each subsequent file is merged
	// into the previous file's keys.
	// log.Debug().Msgf("%v", ko.Strings("config"))
	// if len(ko.Strings("config")) == 0 {

	// 	if err := ko.Load(file.Provider("../tests/config.json"), json.Parser()); err != nil {
	// 		// if err := ko.Load(file.Provider("../.config/config.json"), json.Parser()); err != nil {
	// 		log.Fatal().Msgf("1.error reading config: %v", err)
	// 	} else {
	// 		log.Trace().Msg("1.Successfully read the contents of the config file")
	// 	}
	// }
	for _, f := range ko.Strings("config") {
		log.Debug().Msgf("Reading config from %s", f)
		var parser koanf.Parser
		fileExtension := f[strings.LastIndex(f, ".")+1:]
		switch fileExtension {
		case "yaml":
			parser = yaml.Parser()
		case "json":
			parser = json.Parser()
		default:
			return fmt.Errorf("unsupported file extension")
		}
		if err := ko.Load(file.Provider(f), parser); err != nil {
			log.Fatal().Msgf("error reading config: %v", err)
		} else {
			log.Trace().Msg("Successfully read the contents of the config file")
		}
	}
	return nil
}