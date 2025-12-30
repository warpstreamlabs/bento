package private

type Internal[T any] struct {
	value T
}

func ToInternal[T any](v T) Internal[T] {
	return Internal[T]{value: v}
}

func FromInternal[T any](w Internal[T]) T {
	return w.value
}
