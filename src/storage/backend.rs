//! 存储后端 trait
//!
//! 定义统一的存储后端接口，所有存储实现（稀疏文件、内存等）都必须实现此 trait。

use anyhow::Result;

/// 存储后端 trait
///
/// 抽象了数据块的创建、读写和刷新操作。
/// 所有方法默认使用同步 I/O，但通过 async trait 暴露以兼容异步调用链。
#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    /// 创建存储空间，预分配 file_size 字节
    async fn create(&mut self, file_size: u64) -> Result<()>;

    /// 在指定偏移量写入数据，返回实际写入字节数
    async fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize>;

    /// 从指定偏移量读取数据到缓冲区，返回实际读取字节数
    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// 返回已完成写入的区间列表 (start, end)
    /// 用于断点续传时确定哪些区域已经下载完成
    fn completed_ranges(&self) -> Vec<(u64, u64)>;

    /// 刷新缓冲区数据到持久化存储
    async fn flush(&mut self) -> Result<()>;

    /// 返回存储路径（仅文件后端有效，内存后端返回 None）
    fn path(&self) -> Option<&std::path::Path>;
}
