package conformance

func closeQuietly[T interface{ Close() error }](value T) {
	_ = value.Close()
}
