use clap::Parser;
use spark_core::download;
use spark_core::network::client::ProxyConfig;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "spark-core")]
#[command(about = "高性能多线程下载器")]
#[command(version)]
struct Args {
    /// 下载URL
    url: String,

    /// 输出文件路径
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// 下载线程数 (1-256)
    #[arg(short, long, default_value = "8")]
    threads: usize,

    /// IO线程数 (1-8)
    #[arg(long, default_value = "2")]
    io_threads: usize,

    /// 缓冲区大小 (KB)
    #[arg(long, default_value = "64")]
    buffer_size: usize,

    /// HTTP/HTTPS 代理地址 (e.g. "http://127.0.0.1:8080")
    /// 也可以通过环境变量 HTTP_PROXY/HTTPS_PROXY 设置
    #[arg(long)]
    proxy: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 设置输出路径
    let output = args.output.unwrap_or_else(|| {
        PathBuf::from(args.url.split('/').next_back().unwrap_or("download"))
    });

    // 代理配置：CLI 参数优先，其次环境变量
    let proxy_config = if let Some(ref proxy_url) = args.proxy {
        ProxyConfig {
            http_proxy: Some(proxy_url.clone()),
            https_proxy: Some(proxy_url.clone()),
            no_proxy: None,
        }
    } else {
        ProxyConfig::from_env()
    };

    if proxy_config.is_enabled() {
        println!("   代理: {}",
            proxy_config.http_proxy.as_deref()
                .or(proxy_config.https_proxy.as_deref())
                .unwrap_or("(未知)"));
    }

    println!("🚀 Spark Core 下载器");
    println!("   URL: {}", args.url);
    println!("   输出: {:?}", output);
    println!("   下载线程: {}", args.threads);
    println!("   IO线程: {}", args.io_threads);
    println!("   缓冲区: {}KB", args.buffer_size);
    println!();

    // 快速下载（传入代理配置）
    download(&args.url, output, args.threads, proxy_config).await?;

    println!("\n✅ 下载完成!");
    Ok(())
}
