//! IO 工作线程池
//!
//! 管理多个 IO 工作线程，通过有界通道接收下载数据并写入存储后端。
//! 支持背压控制：通道满时下载线程自动等待。

use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error};

use crate::storage::backend::StorageBackend;

use super::chunk_downloader::{ControlFlags, IoTask};

/// IO 工作线程池
///
/// 创建 M 个 tokio task，共享一个 mpsc 接收器。
/// 每个 task 从通道读取 IoTask，调用 StorageBackend::write_at 写入。
pub struct IoWorkerPool {
    handles: Vec<JoinHandle<()>>,
    /// 用于提交 IoTask 的发送端（保留供外部提取，可改为提供 sender() 方法）
    _sender: Option<mpsc::Sender<IoTask>>,
}

impl IoWorkerPool {
    /// 创建并启动 IO 工作线程池
    ///
    /// # Arguments
    /// * `thread_count` - IO 线程数
    /// * `storage` - 存储后端（线程安全 Arc）
    /// * `channel_capacity` - IO 通道容量（背压控制）
    /// * `control` - 停止控制标志
    pub fn spawn(
        thread_count: usize,
        storage: Arc<tokio::sync::Mutex<dyn StorageBackend>>,
        channel_capacity: usize,
        control: Arc<ControlFlags>,
    ) -> (Self, mpsc::Sender<IoTask>) {
        let (io_tx, io_rx) = mpsc::channel::<IoTask>(channel_capacity);
        let io_rx = Arc::new(tokio::sync::Mutex::new(io_rx));

        let mut handles = Vec::with_capacity(thread_count);

        for io_id in 0..thread_count {
            let storage = Arc::clone(&storage);
            let control = Arc::clone(&control);
            let io_rx = Arc::clone(&io_rx);

            let handle = tokio::spawn(async move {
                loop {
                    if control.is_stopped() {
                        break;
                    }

                    let task = {
                        let mut rx = io_rx.lock().await;
                        rx.recv().await
                    };
                    match task {
                        Some(IoTask { offset, data, chunk_index }) => {
                            debug!("IO 工作线程 {} 写入分片 {} 偏移量 {}", io_id, chunk_index, offset);

                            let mut backend = storage.lock().await;
                            if let Err(e) = backend.write_at(offset, &data).await {
                                error!(
                                    "写入分片 {} 偏移量 {} 失败: {}",
                                    chunk_index, offset, e
                                );
                            }
                        }
                        None => {
                            // 通道关闭，正常退出
                            break;
                        }
                    }
                }

                debug!("IO 工作线程 {} 退出", io_id);
            });

            handles.push(handle);
        }

        let pool = Self { handles, _sender: Some(io_tx.clone()) };
        (pool, io_tx)
    }

    /// 获取 IO 提交通道（用于从外部提交 IoTask）
    pub fn sender(&self) -> Option<&mpsc::Sender<IoTask>> {
        self._sender.as_ref()
    }

    /// 等待所有 IO 线程完成
    pub async fn join_all(self) -> Result<()> {
        // 先关闭 sender，所有 clone 也需要被 drop
        // （在调用方 drop io_tx 后 recv() 返回 None，线程退出）
        for handle in self.handles {
            handle
                .await
                .map_err(|e| anyhow!("IO 线程 panic: {}", e))?;
        }
        Ok(())
    }
}
