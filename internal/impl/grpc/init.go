package grpc

func init() {
	if err := Register(); err != nil {
		panic(err)
	}
}
