ARG SERIVCE_PATH="/go/src/cap/upload-service"

################## 1st Build Stage ####################
FROM golang:1.7.3 AS builder
LABEL stage=builder

WORKDIR $(SERIVCE_PATH)
ADD . .

ENV GO111MODULE=on

# Cache go mods based on go.sum/go.mod files
RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux go build -a -o upload-service

################## 2nd Build Stage ####################

FROM busybox:1-glibc

COPY --from=builder $(SERIVCE_PATH)/upload-service /usr/local/bin/task-handler-service
# COPY --from=builder $(SERIVCE_PATH)/config/config.yaml /usr/local/bin/config.yaml

ENTRYPOINT ["./usr/bin/task-handler-service"]
