module github.com/pingcap/tidb-tools

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/Shopify/sarama v1.16.0
	github.com/Shopify/toxiproxy v2.1.3+incompatible // indirect
	github.com/beorn7/perks v0.0.0-20180321164747-3a771d992973
	github.com/chzyer/readline v0.0.0-20171208011716-f6d7a1f6fbf3 // indirect
	github.com/cockroachdb/cmux v0.0.0-20160228191917-112f0506e774
	github.com/codahale/hdrhistogram v0.0.0-20161010025455-3a0bb77429bd
	github.com/coreos/bbolt v1.3.1-coreos.6
	github.com/coreos/etcd v3.2.14+incompatible
	github.com/coreos/go-semver v0.2.0
	github.com/coreos/go-systemd v0.0.0-20180202092358-40e2722dffea
	github.com/coreos/pkg v0.0.0-20160727233714-3ac0863d7acf
	github.com/cznic/golex v0.0.0-20170803123110-4ab7c5e190e4 // indirect
	github.com/cznic/mathutil v0.0.0-20180103181919-c90ba19bea89
	github.com/cznic/parser v0.0.0-20160622100904-31edd927e5b1 // indirect
	github.com/cznic/sortutil v0.0.0-20150617083342-4c7342852e65 // indirect
	github.com/cznic/strutil v0.0.0-20171016134553-529a34b1c186 // indirect
	github.com/cznic/y v0.0.0-20170802143616-045f81c6662a // indirect
	github.com/davecgh/go-spew v1.1.1
	github.com/dgrijalva/jwt-go v3.2.0+incompatible
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/eapache/go-resiliency v1.1.0
	github.com/eapache/go-xerial-snappy v0.0.0-20160609142408-bb955e01b934
	github.com/eapache/queue v0.0.0-20180227141424-093482f3f8ce
	github.com/eknkc/amber v0.0.0-20171010120322-cdade1c07385 // indirect
	github.com/etcd-io/gofail v0.0.0-20180808172546-51ce9a71510a // indirect
	github.com/ghodss/yaml v1.0.0
	github.com/go-sql-driver/mysql v0.0.0-20161129053045-4ac31a97ccff
	github.com/gogo/protobuf v1.0.0
	github.com/golang/glog v0.0.0-20160126235308-23def4e6c14b // indirect
	github.com/golang/groupcache v0.0.0-20160516000752-02826c3e7903
	github.com/golang/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
	github.com/golang/protobuf v1.2.0
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db
	github.com/google/btree v0.0.0-20180813153112-4030bb1f1f0c
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2 // indirect
	github.com/gorilla/websocket v1.2.0 // indirect
	github.com/grpc-ecosystem/go-grpc-middleware v1.0.0 // indirect
	github.com/grpc-ecosystem/go-grpc-prometheus v0.0.0-20160910222444-6b7015e65d36
	github.com/grpc-ecosystem/grpc-gateway v1.4.1
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/jonboulle/clockwork v0.1.0
	github.com/juju/errors v0.0.0-20160809030848-6f54ff631840
	github.com/juju/loggo v0.0.0-20180524022052-584905176618 // indirect
	github.com/juju/testing v0.0.0-20180920084828-472a3e8b2073 // indirect
	github.com/kr/pretty v0.1.0 // indirect
	github.com/mattn/go-shellwords v1.0.3 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808 // indirect
	github.com/ngaut/log v0.0.0-20160810023011-cec23d3e10b0
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7 // indirect
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/onsi/gomega v1.4.2 // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/petar/GoLLRB v0.0.0-20130427215148-53be0d36a84c // indirect
	github.com/pierrec/lz4 v2.0.2+incompatible
	github.com/pierrec/xxHash v0.1.1 // indirect
	github.com/pingcap/check v0.0.0-20171206051426-1c287c953996
	github.com/pingcap/errcode v0.0.0-20180921232412-a1a7271709d9 // indirect
	github.com/pingcap/errors v0.10.1 // indirect
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20180930052200-fae11119f066
	github.com/pingcap/pd v2.1.0-rc.4+incompatible
	github.com/pingcap/tidb v2.0.8+incompatible
	github.com/pingcap/tipb v0.0.0-20171213095807-07ff5b094233
	github.com/pkg/errors v0.8.0
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/client_golang v0.8.0
	github.com/prometheus/client_model v0.0.0-20171117100541-99fa1f4be8e5
	github.com/prometheus/common v0.0.0-20180426121432-d811d2e9bf89
	github.com/prometheus/procfs v0.0.0-20180408092902-8b1c2da0d56d
	github.com/rcrowley/go-metrics v0.0.0-20180503174638-e2704e165165
	github.com/satori/go.uuid v1.2.0
	github.com/siddontang/go v0.0.0-20170517070808-cb568a3e5cc0
	github.com/sirupsen/logrus v1.0.5
	github.com/soheilhy/cmux v0.1.4 // indirect
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/spf13/cobra v0.0.2 // indirect
	github.com/spf13/pflag v1.0.1 // indirect
	github.com/stretchr/testify v1.2.2 // indirect
	github.com/syndtr/goleveldb v0.0.0-20180815032940-ae2bd5eed72d // indirect
	github.com/tmc/grpc-websocket-proxy v0.0.0-20171017195756-830351dc03c6 // indirect
	github.com/twinj/uuid v0.1.0
	github.com/uber-go/atomic v1.3.2 // indirect
	github.com/uber/jaeger-client-go v0.0.0-20180607151842-f7e0d4744fa6
	github.com/uber/jaeger-lib v0.0.0-20180112221534-34d9cc24e47a
	github.com/ugorji/go v1.1.1
	github.com/unrolled/render v0.0.0-20180914162206-b9786414de4d // indirect
	github.com/urfave/negroni v1.0.0 // indirect
	github.com/xiang90/probing v0.0.0-20160813154853-07dd2e8dfe18
	go.uber.org/atomic v1.3.2 // indirect
	go.uber.org/multierr v1.1.0 // indirect
	go.uber.org/zap v1.8.0 // indirect
	golang.org/x/crypto v0.0.0-20180503215945-1f94bef427e3
	golang.org/x/lint v0.0.0-20181026193005-c67002cb31c3 // indirect
	golang.org/x/net v0.0.0-20180906233101-161cd47e91fd
	golang.org/x/sys v0.0.0-20180909124046-d0be0721c37e
	golang.org/x/text v0.3.0
	golang.org/x/time v0.0.0-20180412165947-fbb02b2291d2
	golang.org/x/tools v0.0.0-20181026183834-f60e5f99f081 // indirect
	google.golang.org/genproto v0.0.0-20180427144745-86e600f69ee4
	google.golang.org/grpc v1.12.2
	gopkg.in/airbrake/gobrake.v2 v2.0.9 // indirect
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127 // indirect
	gopkg.in/gemnasium/logrus-airbrake-hook.v2 v2.1.2 // indirect
	gopkg.in/mgo.v2 v2.0.0-20180705113604-9856a29383ce // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0-20170531160350-a96e63847dc3
	gopkg.in/yaml.v2 v2.2.1
)
