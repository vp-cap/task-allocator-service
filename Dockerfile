ARG SERIVCE_PATH="/go/src/vp-cap/task-allocator-service"

################## 1st Build Stage ####################
FROM golang:1.15 AS builder
LABEL stage=builder

WORKDIR $(SERIVCE_PATH)
ADD . .

ENV GO111MODULE=on

# Cache go mods based on go.sum/go.mod files
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -o task-allocator-service

################## 2nd Build Stage ####################

FROM busybox:1-glibc

COPY --from=builder $(SERIVCE_PATH)/upload-service /usr/local/bin/task-allocator-service
# COPY --from=builder $(SERIVCE_PATH)/config/config.yaml /usr/local/bin/config.yaml

ENTRYPOINT ["./usr/bin/task-allocator-service"]
