package bench

func closeQuietly[T interface{ Close() error }](value T) {
	_ = value.Close()
}
