FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
RUN go mod download
COPY . .
RUN mkdir -p /out && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/message-viewer .

FROM alpine:3.21
RUN addgroup -S viewer && adduser -S viewer -G viewer
COPY --from=build /out/message-viewer /usr/local/bin/message-viewer
USER viewer
EXPOSE 8080
ENV LISTEN_ADDR=0.0.0.0:8080
ENTRYPOINT ["/usr/local/bin/message-viewer"]
