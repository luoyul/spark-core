//! 缓冲区管理
//!
//! 提供高效的缓冲区分配和回收

use bytes::BytesMut;

/// 缓冲区池
#[derive(Default)]
pub struct BufferPool;

impl BufferPool {
    /// 创建新的缓冲区池
    pub fn new() -> Self {
        Self
    }

    /// 获取缓冲区
    pub fn get(&self) -> BytesMut {
        BytesMut::with_capacity(8192)
    }
}

/// 环形缓冲区
///
/// 使用 bytes::BytesMut 实现的环形缓冲区，支持读写分离和背压机制
pub struct RingBuffer {
    buf: BytesMut,
    capacity: usize,
    read_pos: usize,
    write_pos: usize,
    size: usize,
}

impl RingBuffer {
    /// 创建新的环形缓冲区
    ///
    /// # Arguments
    /// * `capacity` - 缓冲区容量（字节）
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: BytesMut::with_capacity(capacity),
            capacity,
            read_pos: 0,
            write_pos: 0,
            size: 0,
        }
    }

    /// 写入数据到缓冲区
    ///
    /// 当缓冲区满时返回 0（背压机制），调用者需要等待
    ///
    /// # Arguments
    /// * `data` - 要写入的数据
    ///
    /// # Returns
    /// 实际写入的字节数
    pub fn write(&mut self, data: &[u8]) -> usize {
        let available = self.available_write();
        if available == 0 {
            // 背压：缓冲区已满，返回0让调用者等待
            return 0;
        }

        let to_write = data.len().min(available);

        for (i, byte) in data.iter().enumerate().take(to_write) {
            let pos = (self.write_pos + i) % self.capacity;
            // 确保缓冲区有足够长度
            if self.buf.len() <= pos {
                self.buf.resize(pos + 1, 0);
            }
            self.buf[pos] = *byte;
        }

        self.write_pos = (self.write_pos + to_write) % self.capacity;
        self.size += to_write;

        to_write
    }

    /// 从缓冲区读取数据
    ///
    /// # Arguments
    /// * `buf` - 读取目标缓冲区
    ///
    /// # Returns
    /// 实际读取的字节数
    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        let available = self.available_read();
        if available == 0 {
            return 0;
        }

        let to_read = buf.len().min(available);

        for (i, slot) in buf.iter_mut().enumerate().take(to_read) {
            let pos = (self.read_pos + i) % self.capacity;
            *slot = self.buf[pos];
        }

        self.read_pos = (self.read_pos + to_read) % self.capacity;
        self.size -= to_read;

        to_read
    }

    /// 获取可读取的字节数
    pub fn available_read(&self) -> usize {
        self.size
    }

    /// 获取可写入的字节数
    pub fn available_write(&self) -> usize {
        self.capacity - self.size
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.read_pos = 0;
        self.write_pos = 0;
        self.size = 0;
        self.buf.clear();
    }
}
