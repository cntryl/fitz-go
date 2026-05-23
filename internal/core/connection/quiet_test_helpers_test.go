package connection_test

func closeQuietly[T interface{ Close() error }](value T) {
	_ = value.Close()
}
