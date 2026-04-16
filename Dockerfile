FROM golang:1.26.2-alpine3.23 AS builder

WORKDIR /build
COPY . .

RUN GOOS=linux GOARCH=arm64 go build -o cestorage ./app/cestorage/...

FROM public.ecr.aws/docker/library/alpine:3.23

COPY --from=builder /build/cestorage /cestorage

ENTRYPOINT /cestorage