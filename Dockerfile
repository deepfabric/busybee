FROM deepfabric/build as builder

COPY . /root/go/src/github.com/deepfabric/busybee
WORKDIR /root/go/src/github.com/deepfabric/busybee

RUN make busybee

FROM deepfabric/centos
COPY --from=builder /root/go/src/github.com/deepfabric/busybee/dist/busybee /usr/local/bin/busybee

ENTRYPOINT ["/usr/local/bin/busybee"]