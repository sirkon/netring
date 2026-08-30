package testeasy

import (
	"fmt"
	"io"
	"os"

	"github.com/sirkon/blog"
)

func NewLogger(w io.Writer) *blog.Logger {
	_, needJSONLogging := os.LookupEnv("JSON_LOGGING")
	if needJSONLogging {
		logger, err := blog.NewLogger(blog.NewJSONWriter(w))
		if err != nil {
			_, _ = fmt.Fprintln(os.Stderr, "failed to initialize JSON logger:", err)
			os.Exit(1)
		}

		return logger
	}

	logger, err := blog.NewLogger(blog.NewPrettyWriter(w))
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, "failed to initialize pretty logger:", err)
		os.Exit(1)
	}

	return logger
}
