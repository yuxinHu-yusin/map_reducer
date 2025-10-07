FROM golang:1.22 as build
WORKDIR /app
COPY . .
RUN go mod tidy && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o server .

FROM gcr.io/distroless/base-debian12
WORKDIR /app
COPY --from=build /app/server /app/server
EXPOSE 8080
ENTRYPOINT ["/app/server"]
