FROM ubuntu:jammy

ENV GOPATH /go

RUN apt update && apt install -y golang git wget

RUN wget -O /usr/local/bin/dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.2/dumb-init_1.2.2_amd64 \
 && chmod +x /usr/local/bin/dumb-init

RUN wget https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_amd64.deb \
 && dpkg -i foundationdb*.deb

RUN mkdir -p /go/src/github.com/pingcap/go-ycsb
WORKDIR /go/src/github.com/pingcap/go-ycsb

COPY go.mod .
COPY go.sum .

RUN GO111MODULE=on go mod download

COPY . .

RUN GO11MODULE=on go build -tags "foundationdb release" -o /go-ycsb ./cmd/*

FROM ubuntu:jammy

COPY --from=0 /go-ycsb /go-ycsb
COPY --from=0 /usr/local/bin/dumb-init /usr/local/bin/dumb-init

RUN apt update && apt install -y wget && apt-get purge -y --auto-remove \
 && wget https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_amd64.deb \
 && dpkg -i foundationdb*.deb

ADD workloads /workloads
ADD run.sh /run.sh

EXPOSE 6060

ENTRYPOINT [ "/run.sh" ]
