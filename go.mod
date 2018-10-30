module github.com/pingcap/tidb-tools

require (
	github.com/BurntSushi/toml v0.0.0-20160717150709-99064174e013
	github.com/Shopify/sarama v1.16.0
	github.com/Shopify/toxiproxy v2.1.3+incompatible // indirect
	github.com/beorn7/perks v0.0.0-20160804104726-4c0e84591b9a // indirect
	github.com/cockroachdb/cmux v0.0.0-20160228191917-112f0506e774 // indirect
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd // indirect
	github.com/coreos/bbolt v1.3.1-coreos.5 // indirect
	github.com/coreos/etcd v3.2.14+incompatible
	github.com/coreos/go-semver v0.2.0 // indirect
	github.com/coreos/go-systemd v0.0.0-20161114122254-48702e0da86b // indirect
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf // indirect
	github.com/cznic/golex v0.0.0-20170803123110-4ab7c5e190e4 // indirect
	github.com/cznic/mathutil v0.0.0-20180103181919-c90ba19bea89 // indirect
	github.com/cznic/parser v0.0.0-20160622100904-31edd927e5b1 // indirect
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65 // indirect
	github.com/cznic/strutil v0.0.0-20171016134553-529a34b1c186 // indirect
	github.com/cznic/y v0.0.0-20170802143616-045f81c6662a // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgrijalva/jwt-go v3.0.0+incompatible // indirect
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eapache/go-resiliency v1.1.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20160609142408-bb955e01b934 // indirect
	github.com/eapache/queue v0.0.0-20180227141424-093482f3f8ce // indirect
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/etcd-io/gofail v0.0.0-20180808172546-51ce9a71510a // indirect
	github.com/ghodss/yaml v1.0.0 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20161129053045-4ac31a97ccff
	github.com/gogo/protobuf v0.0.0-20161210182026-06ec6c31ff1b // indirect
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20160516000752-02826c3e7903 // indirect
	github.com/golang/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20150730031844-723cc1e459b8 // indirect
	github.com/google/btree v0.0.0-20161217183710-316fb6d3f031 // indirect
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20160910222444-6b7015e65d36 // indirect
	github.com/grpc-ecosystem/grpc-gateway v1.3.0 // indirect
	github.com/jonboulle/clockwork v0.1.0 // indirect
	github.com/juju/errors v0.0.0-20160809030848-6f54ff631840
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808 // indirect
	github.com/ngaut/log v0.0.0-20160810023011-cec23d3e10b0
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7 // indirect
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/onsi/gomega v1.4.2 // indirect
	github.com/opentracing/opentracing-go v1.0.2 // indirect
	github.com/pierrec/lz4 v2.0.2+incompatible // indirect
	github.com/pierrec/xxHash v0.1.1 // indirect
	github.com/pingcap/check v0.0.0-20161122095354-9b266636177e
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e // indirect
	github.com/pingcap/kvproto v0.0.0-20180123023251-7b013aefd721
	github.com/pingcap/pd v0.0.0-20180619050643-0ec6ffcf94e8
	github.com/pingcap/tidb v2.0.8+incompatible
	github.com/pingcap/tipb v0.0.0-20171213095807-07ff5b094233
	github.com/pkg/errors v0.8.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v0.8.0 // indirect
	github.com/prometheus/client_model v0.0.0-20150212101744-fa8ad6fec335 // indirect
	github.com/prometheus/common v0.0.0-20160623151427-4402f4e5ea79 // indirect
	github.com/prometheus/procfs v0.0.0-20160411190841-abf152e5f3e9 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20180503174638-e2704e165165 // indirect
	github.com/siddontang/go v0.0.0-20170517070808-cb568a3e5cc0
	github.com/sirupsen/logrus v0.0.0-20180618111419-75068beb13ad // indirect
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72 // indirect
	github.com/stretchr/testify v1.2.2 // indirect
	github.com/twinj/uuid v0.1.0 // indirect
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v0.0.0-20180607151842-f7e0d4744fa6 // indirect
	github.com/uber/jaeger-lib v0.0.0-20180112221534-34d9cc24e47a // indirect
	github.com/ugorji/go v0.0.0-20170107133203-ded73eae5db7 // indirect
	github.com/unrolled/render v0.0.0-20180914162206-b9786414de4d // indirect
	github.com/urfave/negroni v1.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18 // indirect
	go.uber.org/atomic v1.3.2 // indirect
	golang.org/x/crypto v0.0.0-20150218234220-1351f936d976 // indirect
	golang.org/x/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/time v0.0.0-20170420181420-c06e80d9300e // indirect
	golang.org/x/tools v0.0.0-20181026183834-f60e5f99f081 // indirect
	google.golang.org/genproto v0.0.0-20170711235230-b0a3dcfcd1a9 // indirect
	google.golang.org/grpc v1.7.5 // indirect
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0-20170531160350-a96e63847dc3 // indirect
)
