FROM golang:1.21-alpine

RUN apk update

WORKDIR /app

COPY go.mod ./

RUN go mod download

COPY . .

RUN go build -o /b4_discovery

EXPOSE 5050

ENTRYPOINT ["/b4_discovery"]