//! 下载任务定义
//!
//! 定义单个下载任务的属性、状态和管理

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use thiserror::Error;

/// 下载范围 (用于分片下载)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Range {
    /// 起始字节位置 (包含)
    pub start: u64,
    /// 结束字节位置 (不包含)
    pub end: u64,
}

impl Range {
    /// 创建新的下载范围
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// 获取范围大小
    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// 检查是否为空范围
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// 转换为HTTP Range头格式
    pub fn to_http_range(&self) -> String {
        format!("bytes={}-{}", self.start, self.end - 1)
    }
}

/// 任务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TaskStatus {
    /// 等待中
    Pending,
    /// 下载中
    Downloading,
    /// 已完成
    Completed,
    /// 失败
    Failed,
    /// 已取消
    Cancelled,
}

/// 任务结果
#[derive(Debug, Clone)]
pub enum TaskResult {
    /// 成功
    Success {
        /// 写入的字节数
        bytes_written: u64,
    },
    /// 失败
    Failed {
        /// 错误信息
        error: String,
        /// 是否可重试
        retryable: bool,
    },
    /// 已取消
    Cancelled,
}

impl TaskResult {
    /// 检查是否成功
    pub fn is_success(&self) -> bool {
        matches!(self, TaskResult::Success { .. })
    }

    /// 检查是否失败
    pub fn is_failed(&self) -> bool {
        matches!(self, TaskResult::Failed { .. })
    }

    /// 检查是否被取消
    pub fn is_cancelled(&self) -> bool {
        matches!(self, TaskResult::Cancelled)
    }

    /// 获取写入的字节数 (成功时)
    pub fn bytes_written(&self) -> Option<u64> {
        match self {
            TaskResult::Success { bytes_written } => Some(*bytes_written),
            _ => None,
        }
    }
}

/// 下载任务
#[derive(Debug, Clone)]
pub struct DownloadTask {
    /// 任务ID
    pub id: u64,
    /// 目标URL
    pub url: String,
    /// 下载范围 (用于分片)
    pub range: Range,
    /// 本地输出路径
    pub output_path: PathBuf,
    /// 优先级 (0-255, 数值越大优先级越高)
    pub priority: u8,
    /// 当前重试次数
    pub retry_count: u8,
    /// 最大重试次数
    pub max_retries: u8,
    /// 当前状态
    pub status: TaskStatus,
    /// 任务结果 (完成后设置)
    pub result: Option<TaskResult>,
}

impl DownloadTask {
    /// 创建新的下载任务
    pub fn new(
        id: u64,
        url: impl Into<String>,
        range: Range,
        output_path: impl Into<PathBuf>,
        priority: u8,
        max_retries: u8,
    ) -> Self {
        Self {
            id,
            url: url.into(),
            range,
            output_path: output_path.into(),
            priority,
            retry_count: 0,
            max_retries,
            status: TaskStatus::Pending,
            result: None,
        }
    }

    /// 增加重试计数
    pub fn increment_retry(&mut self) {
        self.retry_count += 1;
    }

    /// 检查是否还可以重试
    pub fn can_retry(&self) -> bool {
        self.retry_count < self.max_retries
    }

    /// 设置状态
    pub fn set_status(&mut self, status: TaskStatus) {
        self.status = status;
    }

    /// 设置结果
    pub fn set_result(&mut self, result: TaskResult) {
        self.result = Some(result);
        self.status = match &self.result {
            Some(TaskResult::Success { .. }) => TaskStatus::Completed,
            Some(TaskResult::Failed { .. }) => TaskStatus::Failed,
            Some(TaskResult::Cancelled) => TaskStatus::Cancelled,
            None => self.status,
        };
    }

    /// 标记为取消
    pub fn cancel(&mut self) {
        self.status = TaskStatus::Cancelled;
        self.result = Some(TaskResult::Cancelled);
    }

    /// 获取任务总大小
    pub fn size(&self) -> u64 {
        self.range.size()
    }
}

/// 任务管理器错误
#[derive(Debug, Error)]
pub enum TaskManagerError {
    #[error("任务不存在: {0}")]
    TaskNotFound(u64),
    #[error("任务已存在: {0}")]
    TaskAlreadyExists(u64),
}

/// 任务管理器
///
/// 管理所有下载任务的元数据，使用RwLock<HashMap>实现并发安全
#[derive(Debug, Clone)]
pub struct TaskManager {
    /// 任务存储
    tasks: Arc<RwLock<HashMap<u64, DownloadTask>>>,
    /// 任务ID生成器
    id_counter: Arc<AtomicU64>,
}

impl Default for TaskManager {
    fn default() -> Self {
        Self::new()
    }
}

impl TaskManager {
    /// 创建新的任务管理器
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
            id_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    /// 生成新的任务ID
    pub fn next_id(&self) -> u64 {
        self.id_counter.fetch_add(1, Ordering::SeqCst)
    }

    /// 添加任务
    ///
    /// 如果任务ID已存在，返回错误
    pub fn add_task(&self, task: DownloadTask) -> Result<(), TaskManagerError> {
        let mut tasks = self.tasks.write();
        if tasks.contains_key(&task.id) {
            return Err(TaskManagerError::TaskAlreadyExists(task.id));
        }
        tasks.insert(task.id, task);
        Ok(())
    }

    /// 获取任务状态
    pub fn get_task_status(&self, id: u64) -> Option<TaskStatus> {
        let tasks = self.tasks.read();
        tasks.get(&id).map(|task| task.status)
    }

    /// 获取任务完整信息
    pub fn get_task(&self, id: u64) -> Option<DownloadTask> {
        let tasks = self.tasks.read();
        tasks.get(&id).cloned()
    }

    /// 取消任务
    ///
    /// 将任务状态设置为Cancelled
    pub fn cancel_task(&self, id: u64) -> Result<(), TaskManagerError> {
        let mut tasks = self.tasks.write();
        let task = tasks
            .get_mut(&id)
            .ok_or(TaskManagerError::TaskNotFound(id))?;

        // 只有Pending或Downloading状态的任务可以取消
        match task.status {
            TaskStatus::Pending | TaskStatus::Downloading => {
                task.cancel();
                Ok(())
            }
            _ => Ok(()), // 已完成或已失败的任务无需操作
        }
    }

    /// 更新任务
    pub fn update_task(&self, task: DownloadTask) -> Result<(), TaskManagerError> {
        let mut tasks = self.tasks.write();
        if !tasks.contains_key(&task.id) {
            return Err(TaskManagerError::TaskNotFound(task.id));
        }
        tasks.insert(task.id, task);
        Ok(())
    }

    /// 获取所有任务
    pub fn get_all_tasks(&self) -> Vec<DownloadTask> {
        let tasks = self.tasks.read();
        tasks.values().cloned().collect()
    }

    /// 获取指定状态的任务
    pub fn get_tasks_by_status(&self, status: TaskStatus) -> Vec<DownloadTask> {
        let tasks = self.tasks.read();
        tasks
            .values()
            .filter(|t| t.status == status)
            .cloned()
            .collect()
    }

    /// 获取待处理的任务 (按优先级排序)
    pub fn get_pending_tasks(&self) -> Vec<DownloadTask> {
        let mut tasks: Vec<_> = self.get_tasks_by_status(TaskStatus::Pending);
        tasks.sort_by(|a, b| b.priority.cmp(&a.priority)); // 高优先级在前
        tasks
    }

    /// 移除任务
    pub fn remove_task(&self, id: u64) -> Result<DownloadTask, TaskManagerError> {
        let mut tasks = self.tasks.write();
        tasks.remove(&id).ok_or(TaskManagerError::TaskNotFound(id))
    }

    /// 获取任务数量
    pub fn task_count(&self) -> usize {
        let tasks = self.tasks.read();
        tasks.len()
    }

    /// 清除已完成的任务
    pub fn clear_completed(&self) -> usize {
        let mut tasks = self.tasks.write();
        let before = tasks.len();
        tasks.retain(|_, task| {
            !matches!(task.status, TaskStatus::Completed | TaskStatus::Cancelled)
        });
        before - tasks.len()
    }
}

/// 任务分片配置
#[derive(Debug, Clone, Copy)]
pub struct SplitConfig {
    /// 每个分片的最小大小 (字节)
    pub min_chunk_size: u64,
    /// 每个分片的最大大小 (字节)
    pub max_chunk_size: u64,
    /// 最大分片数量
    pub max_chunks: usize,
}

impl Default for SplitConfig {
    fn default() -> Self {
        Self {
            min_chunk_size: 1024 * 1024,       // 1MB
            max_chunk_size: 100 * 1024 * 1024, // 100MB
            max_chunks: 16,
        }
    }
}

impl SplitConfig {
    /// 创建新的分片配置
    pub fn new(min_chunk_size: u64, max_chunk_size: u64, max_chunks: usize) -> Self {
        Self {
            min_chunk_size,
            max_chunk_size,
            max_chunks,
        }
    }
}

/// 任务分片器
///
/// 将大文件分割为多个Range任务
pub struct TaskSplitter {
    config: SplitConfig,
}

impl TaskSplitter {
    /// 使用默认配置创建分片器
    pub fn new() -> Self {
        Self {
            config: SplitConfig::default(),
        }
    }

    /// 使用自定义配置创建分片器
    pub fn with_config(config: SplitConfig) -> Self {
        Self { config }
    }

    /// 将文件分割为多个下载任务
    ///
    /// # 参数
    /// - `url`: 下载URL
    /// - `file_size`: 文件总大小
    /// - `output_dir`: 输出目录
    /// - `base_filename`: 基础文件名
    /// - `task_manager`: 任务管理器
    /// - `priority`: 任务优先级
    /// - `max_retries`: 最大重试次数
    ///
    /// # 返回
    /// 返回创建的任务ID列表
    #[allow(clippy::too_many_arguments)]
    pub fn split_file(
        &self,
        url: impl Into<String>,
        file_size: u64,
        output_dir: impl Into<PathBuf>,
        base_filename: impl AsRef<str>,
        task_manager: &TaskManager,
        priority: u8,
        max_retries: u8,
    ) -> Vec<u64> {
        let url = url.into();
        let output_dir: PathBuf = output_dir.into();
        let base_filename = base_filename.as_ref();

        // 计算分片大小和数量
        let chunk_size = self.calculate_chunk_size(file_size);
        let num_chunks = file_size.div_ceil(chunk_size) as usize;

        let mut task_ids = Vec::with_capacity(num_chunks);

        for i in 0..num_chunks {
            let start = i as u64 * chunk_size;
            let end = ((i as u64 + 1) * chunk_size).min(file_size);

            let range = Range::new(start, end);

            // 生成分片文件名: filename.part_0, filename.part_1, ...
            let part_filename = format!("{}.part_{}", base_filename, i);
            let output_path = output_dir.join(&part_filename);

            let task_id = task_manager.next_id();
            let task = DownloadTask::new(
                task_id,
                url.clone(),
                range,
                output_path,
                priority,
                max_retries,
            );

            if let Err(e) = task_manager.add_task(task) {
                eprintln!("Failed to add task {}: {}", task_id, e);
                continue;
            }

            task_ids.push(task_id);
        }

        task_ids
    }

    /// 计算分片大小
    fn calculate_chunk_size(&self, file_size: u64) -> u64 {
        if file_size == 0 {
            return self.config.min_chunk_size;
        }

        // 尝试将文件均匀分成max_chunks份
        let ideal_chunk_size = file_size / self.config.max_chunks as u64;

        // 限制在最小和最大分片大小之间
        let chunk_size = ideal_chunk_size
            .max(self.config.min_chunk_size)
            .min(self.config.max_chunk_size);

        // 如果文件很小，直接作为一个分片
        if file_size <= self.config.min_chunk_size {
            return file_size;
        }

        chunk_size
    }

    /// 创建单个不分片的任务
    pub fn create_single_task(
        &self,
        url: impl Into<String>,
        file_size: u64,
        output_path: impl Into<PathBuf>,
        task_manager: &TaskManager,
        priority: u8,
        max_retries: u8,
    ) -> u64 {
        let task_id = task_manager.next_id();
        let task = DownloadTask::new(
            task_id,
            url,
            Range::new(0, file_size),
            output_path,
            priority,
            max_retries,
        );

        if let Err(e) = task_manager.add_task(task) {
            eprintln!("Failed to add task {}: {}", task_id, e);
        }

        task_id
    }
}

impl Default for TaskSplitter {
    fn default() -> Self {
        Self::new()
    }
}

/// 智能分片策略
///
/// 根据网络状况和文件大小动态调整分片策略
pub struct AdaptiveSplitter {
    base_splitter: TaskSplitter,
    /// 启用分片的最小文件大小
    pub min_size_for_split: u64,
}

impl AdaptiveSplitter {
    /// 创建新的自适应分片器
    pub fn new() -> Self {
        Self {
            base_splitter: TaskSplitter::new(),
            min_size_for_split: 10 * 1024 * 1024, // 10MB
        }
    }

    /// 设置启用分片的最小文件大小
    pub fn with_min_size(mut self, size: u64) -> Self {
        self.min_size_for_split = size;
        self
    }

    /// 根据文件大小决定是否分片
    #[allow(clippy::too_many_arguments)]
    pub fn split_or_single(
        &self,
        url: impl Into<String>,
        file_size: u64,
        output_dir: impl Into<PathBuf>,
        base_filename: impl AsRef<str>,
        final_output_path: impl Into<PathBuf>,
        task_manager: &TaskManager,
        priority: u8,
        max_retries: u8,
    ) -> Vec<u64> {
        let url = url.into();

        // 小文件不分片
        if file_size < self.min_size_for_split {
            let task_id = self.base_splitter.create_single_task(
                url,
                file_size,
                final_output_path,
                task_manager,
                priority,
                max_retries,
            );
            return vec![task_id];
        }

        // 大文件分片下载
        self.base_splitter.split_file(
            url,
            file_size,
            output_dir,
            base_filename,
            task_manager,
            priority,
            max_retries,
        )
    }
}

impl Default for AdaptiveSplitter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range() {
        let range = Range::new(0, 1024);
        assert_eq!(range.size(), 1024);
        assert!(!range.is_empty());
        assert_eq!(range.to_http_range(), "bytes=0-1023");
    }

    #[test]
    fn test_task_result() {
        let success = TaskResult::Success { bytes_written: 100 };
        assert!(success.is_success());
        assert!(!success.is_failed());
        assert_eq!(success.bytes_written(), Some(100));

        let failed = TaskResult::Failed {
            error: "test".to_string(),
            retryable: true,
        };
        assert!(!failed.is_success());
        assert!(failed.is_failed());

        let cancelled = TaskResult::Cancelled;
        assert!(cancelled.is_cancelled());
    }

    #[test]
    fn test_task_manager() {
        let manager = TaskManager::new();

        let task = DownloadTask::new(
            manager.next_id(),
            "http://example.com/file.zip",
            Range::new(0, 1024),
            "/tmp/file.zip",
            100,
            3,
        );

        let task_id = task.id;
        manager.add_task(task).unwrap();

        assert_eq!(manager.task_count(), 1);
        assert_eq!(manager.get_task_status(task_id), Some(TaskStatus::Pending));

        manager.cancel_task(task_id).unwrap();
        assert_eq!(
            manager.get_task_status(task_id),
            Some(TaskStatus::Cancelled)
        );
    }

    #[test]
    fn test_task_splitter() {
        let splitter = TaskSplitter::new();
        let manager = TaskManager::new();

        let file_size = 100 * 1024 * 1024; // 100MB
        let task_ids = splitter.split_file(
            "http://example.com/file.zip",
            file_size,
            "/tmp",
            "file.zip",
            &manager,
            100,
            3,
        );

        // 100MB应该被分成多个分片
        assert!(!task_ids.is_empty());

        // 验证所有任务都被创建
        assert_eq!(manager.task_count(), task_ids.len());

        // 验证分片范围连续
        let tasks = manager.get_all_tasks();
        let mut ranges: Vec<_> = tasks.iter().map(|t| t.range).collect();
        ranges.sort_by_key(|r| r.start);

        assert_eq!(ranges.first().unwrap().start, 0);
        assert_eq!(ranges.last().unwrap().end, file_size);
    }

    #[test]
    fn test_adaptive_splitter_small_file() {
        let splitter = AdaptiveSplitter::new();
        let manager = TaskManager::new();

        // 1MB小文件，不应该分片
        let file_size = 1024 * 1024;
        let task_ids = splitter.split_or_single(
            "http://example.com/file.zip",
            file_size,
            "/tmp",
            "file.zip",
            "/tmp/file.zip",
            &manager,
            100,
            3,
        );

        assert_eq!(task_ids.len(), 1);
        assert_eq!(manager.task_count(), 1);
    }
}
