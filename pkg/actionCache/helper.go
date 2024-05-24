package actionCache

import (
	"fmt"
	"golang.org/x/exp/maps"
	"slices"
	"strings"
)

type ActionParams map[string]string

func (ap ActionParams) Get(key string) string {
	val, _ := ap[key]
	return val
}

func (ap ActionParams) Set(key, value string) {
	ap[key] = value
}

func (ap ActionParams) Del(key string) {
	delete(ap, key)
}

func (ap ActionParams) Has(key string) bool {
	_, ok := ap[key]
	return ok
}

func (ap ActionParams) String() string {
	if len(ap) == 0 {
		return ""
	}
	keys := maps.Keys(ap)
	slices.Sort(keys)
	var res string
	for _, key := range keys {
		res += fmt.Sprintf("/%s%s", key, ap.Get(key))
	}
	return res[1:]
}

func (ap ActionParams) SetString(paramStr string, keys []string) {
	maps.Clear(ap)
	parts := strings.Split(strings.ToLower(paramStr), "/")
	for _, key := range keys {
		for _, part := range parts {
			if part == "" {
				continue
			}
			if strings.HasPrefix(part, key) {
				ap.Set(key, part[len(key):])
				break
			}
		}
	}
}
