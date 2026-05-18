//! 内存存储后端
//!
//! 基于 Vec<u8> 的内存存储实现，主要用于单元测试。

use anyhow::Result;
use tokio::sync::RwLock;

use super::backend::StorageBackend;

/// 内存后端，用 Vec<u8> 存储数据，RwLock 保护并发访问
pub struct MemoryBackend {
    data: RwLock<Vec<u8>>,
}

impl MemoryBackend {
    /// 创建新的内存后端
    pub fn new() -> Self {
        Self {
            data: RwLock::new(Vec::new()),
        }
    }
}

impl Default for MemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl StorageBackend for MemoryBackend {
    async fn create(&mut self, file_size: u64) -> Result<()> {
        let mut data = self.data.write().await;
        *data = vec![0u8; file_size as usize];
        Ok(())
    }

    async fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        let mut buf = self.data.write().await;
        let start = offset as usize;
        let end = start + data.len();

        if end > buf.len() {
            buf.resize(end, 0);
        }

        buf[start..end].copy_from_slice(data);
        Ok(data.len())
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let data = self.data.read().await;
        let start = offset as usize;

        if start >= data.len() {
            return Ok(0);
        }

        let available = data.len() - start;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&data[start..start + to_read]);
        Ok(to_read)
    }

    fn completed_ranges(&self) -> Vec<(u64, u64)> {
        // Phase 4 实现区间追踪，目前返回空
        Vec::new()
    }

    async fn flush(&mut self) -> Result<()> {
        // 内存后端无需刷新
        Ok(())
    }

    fn path(&self) -> Option<&std::path::Path> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_backend_create_and_write() {
        let mut backend = MemoryBackend::new();
        backend.create(1024).await.unwrap();

        let data = b"hello world";
        let written = backend.write_at(100, data).await.unwrap();
        assert_eq!(written, data.len());

        let mut buf = vec![0u8; data.len()];
        let read = backend.read_at(100, &mut buf).await.unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_memory_backend_read_beyond_bounds() {
        let mut backend = MemoryBackend::new();
        backend.create(100).await.unwrap();

        let mut buf = vec![0u8; 10];
        let read = backend.read_at(200, &mut buf).await.unwrap();
        assert_eq!(read, 0);
    }

    #[tokio::test]
    async fn test_memory_backend_write_expands() {
        let mut backend = MemoryBackend::new();
        backend.create(10).await.unwrap();

        let data = b"this is a longer string";
        backend.write_at(5, data).await.unwrap();

        let mut buf = vec![0u8; data.len()];
        let read = backend.read_at(5, &mut buf).await.unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_memory_backend_no_path() {
        let backend = MemoryBackend::new();
        assert!(backend.path().is_none());
    }
}
