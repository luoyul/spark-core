# Spark Core 多线程下载引擎

高性能 Rust 多线程下载器，支持下载-IO分离架构和背压控制。

## 特性

- 🚀 **多线程下载**: 1-256 并发线程
- 💾 **下载-IO分离**: 独立线程池避免阻塞
- 🎚️ **背压控制**: 自动限流防止内存堆积
- 📁 **稀疏文件**: 预分配磁盘空间，支持断点续传
- 🔄 **自动重试**: 指数退避重试机制
- ⏸️ **暂停/恢复**: 随时控制下载状态

## 安装

```bash
cargo install --git https://github.com/luoyul/spark-core
```

## 使用

```bash
# 基本下载
spark-core https://example.com/file.zip

# 指定输出路径和线程数
spark-core https://example.com/file.zip -o ./file.zip -t 16

# 高性能配置
spark-core https://example.com/file.zip -t 32 --io-threads 4 --buffer-size 256
```

## 架构

```
┌─────────────────┐     ┌─────────────┐     ┌─────────────┐
│  下载线程 (N)   │────▶│  有界通道   │────▶│  IO 线程(M) │
│  HTTP 下载      │     │  背压控制   │     │  磁盘写入   │
└─────────────────┘     └─────────────┘     └─────────────┘
        │                                            │
        └──────── IO信号量满时自动阻塞 ──────────────┘
```

## 配置

```rust
use spark_core::DownloadConfig;

let config = DownloadConfig::new()
    .thread_count(16)           // 下载线程: 1-256
    .io_thread_count(4)         // IO线程: 1-8
    .buffer_size(64 * 1024)     // 缓冲区大小 (64KB)
    .io_channel_capacity(128)   // IO通道容量
    .retry_limit(3);            // 重试次数
```

## 库使用

```rust
use spark_core::{DownloadConfig, DownloadEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = DownloadConfig::new()
        .thread_count(16);
    
    let engine = DownloadEngine::new(config)?;
    engine.download("https://example.com/file.zip", "/path/to/output").await?;
    
    Ok(())
}
```

## License

MIT
