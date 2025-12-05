# 构建阶段
# 依然建议使用 1.22 或 latest，因为 1.23.7 目前可能无法拉取
FROM golang:1.22 AS backend-builder

WORKDIR /app

# 1. 先复制依赖文件并下载，利用 Docker 缓存层
COPY go.mod go.sum ./
RUN go mod download

# 2. 复制源码并构建
COPY . .
# -ldflags="-s -w" 减小体积
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o sync-tool cmd/main.go

# 最终镜像
# 推荐使用 bookworm-slim (Debian 12) 配合最新的运行时依赖
FROM debian:bookworm-slim

WORKDIR /app

# 3. 安装运行时依赖 (保留这部分优化，防止连接报错和时区问题)
# ca-certificates: 必需，否则 HTTPS/SSL 连数据库会报错
# tzdata: 必需，否则日志时间和数据库时间是 UTC
RUN apt-get update && apt-get install -y \
    ca-certificates \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 设置时区
ENV TZ=Asia/Shanghai

# 4. 复制二进制文件
COPY --from=backend-builder /app/sync-tool .

# 5. 配置文件 (已恢复为你原始的路径写法)
# 既然运行时会挂载，这里保持原样即可
COPY --from=backend-builder /app/configs/config.yml ../configs/

EXPOSE 28081

CMD ["./sync-tool"]
