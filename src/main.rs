use clap::Parser;
use spark_core::download;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    // 设置输出路径
    let output = args.output.unwrap_or_else(|| {
        // 从URL提取文件名
        PathBuf::from(
            args.url
                .split('/')
                .last()
                .unwrap_or("download"),
        )
    });

    println!("🚀 Spark Core 下载器");
    println!("   URL: {}", args.url);
    println!("   输出: {:?}", output);
    println!("   下载线程: {}", args.threads);
    println!("   IO线程: {}", args.io_threads);
    println!("   缓冲区: {}KB", args.buffer_size);
    println!();

    // 快速下载
    download(&args.url, output, args.threads).await?;

    println!("\n✅ 下载完成!");
    Ok(())
}
