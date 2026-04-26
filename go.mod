module github.com/pgx-contrib/pgxaip

go 1.25.8

require (
	github.com/google/cel-go v0.28.0
	github.com/onsi/ginkgo/v2 v2.28.1
	github.com/onsi/gomega v1.39.1
	github.com/pgx-contrib/pgxcel v0.0.0-20260426034612-8214ec06df8f
	go.einride.tech/aip v0.85.0
)

tool github.com/onsi/ginkgo/v2/ginkgo

require (
	cel.dev/expr v0.25.1 // indirect
	github.com/Masterminds/semver/v3 v3.4.0 // indirect
	github.com/antlr4-go/antlr/v4 v4.13.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20260115054156-294ebfa9ad83 // indirect
	go.yaml.in/yaml/v3 v3.0.4 // indirect
	golang.org/x/exp v0.0.0-20240823005443-9b4947da3948 // indirect
	golang.org/x/mod v0.32.0 // indirect
	golang.org/x/net v0.49.0 // indirect
	golang.org/x/sync v0.19.0 // indirect
	golang.org/x/sys v0.40.0 // indirect
	golang.org/x/text v0.33.0 // indirect
	golang.org/x/tools v0.41.0 // indirect
	google.golang.org/genproto/googleapis/api v0.0.0-20250528174236-200df99c418a // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20250528174236-200df99c418a // indirect
	google.golang.org/protobuf v1.36.10 // indirect
)

replace go.einride.tech/aip => github.com/iamralch/aip-go v0.0.0-20260424155005-c6380f4cb9b5

replace github.com/pgx-contrib/pgxcel => ../pgxcel
