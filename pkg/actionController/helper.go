package actionController

import (
	"crypto/sha256"
	"fmt"
	"strings"
)

func CreateCacheName(collection, signature, action, params, ext string) string {
	nameBytes := sha256.Sum256([]byte(strings.TrimRight(fmt.Sprintf("%s/%s/%s/%s", collection, signature, action, params), "/")))
	return fmt.Sprintf("%x.%s", nameBytes, strings.TrimLeft(ext, "."))
}
