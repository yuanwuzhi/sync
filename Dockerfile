# 构建阶段
FROM golang:1.23.7 as backend-builder
WORKDIR /app
COPY . .
RUN CGO_ENABLED=0 go build -o sync-tool cmd/main.go

# 最终镜像
FROM debian:buster-slim
WORKDIR /app
COPY --from=backend-builder /app/sync-tool .
COPY --from=backend-builder /app/configs/config.yml ../configs/

EXPOSE 28081
CMD ["./sync-tool"]
