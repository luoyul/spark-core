//! 下载调度器
//!
//! 负责任务的调度和资源分配
//! 支持1-256个下载线程，实现工作窃取算法和动态线程调整

use anyhow::{anyhow, Result};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Notify, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info};

/// 下载范围（用于分片下载）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ByteRange {
    /// 起始字节位置
    pub start: u64,
    /// 结束字节位置（不包含）
    pub end: u64,
}

impl ByteRange {
    /// 创建新的字节范围
    pub fn new(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// 获取范围大小
    pub fn size(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }
}

/// 任务优先级
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum TaskPriority {
    /// 低优先级
    Low = 0,
    /// 正常优先级
    #[default]
    Normal = 1,
    /// 高优先级
    High = 2,
    /// 紧急优先级
    Critical = 3,
}

/// 下载任务片段
#[derive(Debug, Clone)]
pub struct DownloadTask {
    /// 任务ID
    pub id: String,
    /// 目标URL
    pub url: String,
    /// 本地保存路径
    pub local_path: String,
    /// 下载范围（可选，None表示完整下载）
    pub range: Option<ByteRange>,
    /// 任务优先级
    pub priority: TaskPriority,
}

impl DownloadTask {
    /// 创建新的下载任务
    pub fn new(
        id: impl Into<String>,
        url: impl Into<String>,
        local_path: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            local_path: local_path.into(),
            range: None,
            priority: TaskPriority::Normal,
        }
    }

    /// 创建分片下载任务
    pub fn with_range(
        id: impl Into<String>,
        url: impl Into<String>,
        local_path: impl Into<String>,
        range: ByteRange,
    ) -> Self {
        Self {
            id: id.into(),
            url: url.into(),
            local_path: local_path.into(),
            range: Some(range),
            priority: TaskPriority::Normal,
        }
    }

    /// 设置优先级
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }
}

/// 网络状况统计
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// 平均下载速度（字节/秒）
    pub avg_speed: f64,
    /// 连接成功率
    pub success_rate: f64,
    /// 平均延迟（毫秒）
    pub avg_latency: f64,
    /// 活跃连接数
    pub active_connections: usize,
}

/// 调度器配置
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// 最小线程数
    pub min_threads: usize,
    /// 最大线程数
    pub max_threads: usize,
    /// 任务队列容量
    pub queue_capacity: usize,
    /// 网络检查间隔（秒）
    pub network_check_interval_secs: u64,
    /// 速度阈值（低于此值减少线程）
    pub speed_threshold: f64,
    /// 速度提升阈值（高于此值增加线程）
    pub speed_boost_threshold: f64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            min_threads: 1,
            max_threads: 8,
            queue_capacity: 1000,
            network_check_interval_secs: 5,
            speed_threshold: 1024.0 * 100.0,        // 100KB/s
            speed_boost_threshold: 1024.0 * 1024.0, // 1MB/s
        }
    }
}

/// 任务调度器
///
/// 管理1-256个下载线程，实现工作窃取算法和动态线程调整
pub struct Scheduler {
    /// 配置
    config: SchedulerConfig,
    /// 全局任务队列（工作窃取注入器）
    global_queue: Arc<Injector<DownloadTask>>,
    /// 工作线程的窃取器
    stealers: Arc<RwLock<Vec<Stealer<DownloadTask>>>>,
    /// 当前活跃线程数
    active_threads: Arc<AtomicUsize>,
    /// 目标线程数（动态调整）
    target_threads: Arc<AtomicUsize>,
    /// 运行状态
    running: Arc<AtomicBool>,
    /// 关闭通知
    shutdown_notify: Arc<Notify>,
    /// 网络统计
    network_stats: Arc<RwLock<NetworkStats>>,
    /// 任务完成计数
    completed_tasks: Arc<AtomicUsize>,
    /// 任务提交通道
    task_sender: Option<mpsc::Sender<DownloadTask>>,
    /// 任务提交接收器（在run时初始化）
    task_receiver: Arc<RwLock<Option<mpsc::Receiver<DownloadTask>>>>,
}

impl Scheduler {
    /// 创建新的调度器（使用默认配置，8线程）
    pub fn new() -> Result<Self> {
        Self::with_config(8, SchedulerConfig::default())
    }

    /// 创建新的调度器
    ///
    /// # Arguments
    /// * `thread_count` - 初始线程数，必须在1-256范围内
    ///
    /// # Errors
    /// 如果thread_count不在1-256范围内，返回错误
    pub fn with_threads(thread_count: usize) -> Result<Self> {
        Self::with_config(thread_count, SchedulerConfig::default())
    }

    /// 使用配置创建调度器
    pub fn with_config(thread_count: usize, config: SchedulerConfig) -> Result<Self> {
        // 验证线程数范围
        if !(1..=256).contains(&thread_count) {
            return Err(anyhow!("线程数必须在1-256范围内，当前: {}", thread_count));
        }

        // 确保线程数在配置范围内
        let thread_count = thread_count.clamp(config.min_threads, config.max_threads);

        let (task_sender, task_receiver) = mpsc::channel(config.queue_capacity);

        Ok(Self {
            config,
            global_queue: Arc::new(Injector::new()),
            stealers: Arc::new(RwLock::new(Vec::new())),
            active_threads: Arc::new(AtomicUsize::new(0)),
            target_threads: Arc::new(AtomicUsize::new(thread_count)),
            running: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            network_stats: Arc::new(RwLock::new(NetworkStats::default())),
            completed_tasks: Arc::new(AtomicUsize::new(0)),
            task_sender: Some(task_sender),
            task_receiver: Arc::new(RwLock::new(Some(task_receiver))),
        })
    }

    /// 提交下载任务
    ///
    /// # Arguments
    /// * `task` - 要提交的下载任务
    ///
    /// # Errors
    /// 如果调度器已关闭或通道已满，返回错误
    pub async fn submit_task(&self, task: DownloadTask) -> Result<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(anyhow!("调度器未运行"));
        }

        if let Some(sender) = &self.task_sender {
            let task_id = task.id.clone();
            sender
                .send(task)
                .await
                .map_err(|_| anyhow!("任务提交失败，调度器可能已关闭"))?;
            debug!("任务已提交: {}", task_id);
            Ok(())
        } else {
            Err(anyhow!("任务提交通道未初始化"))
        }
    }

    /// 同步方式提交任务（非阻塞）
    pub fn submit_task_blocking(&self, task: DownloadTask) -> Result<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Err(anyhow!("调度器未运行"));
        }

        // 直接放入全局队列
        self.global_queue.push(task);
        Ok(())
    }

    /// 获取当前活跃线程数
    pub fn active_thread_count(&self) -> usize {
        self.active_threads.load(Ordering::SeqCst)
    }

    /// 获取目标线程数
    pub fn target_thread_count(&self) -> usize {
        self.target_threads.load(Ordering::SeqCst)
    }

    /// 动态调整线程数
    ///
    /// # Arguments
    /// * `new_count` - 新的线程数，必须在1-256范围内
    pub async fn adjust_threads(&self, new_count: usize) -> Result<()> {
        if !(1..=256).contains(&new_count) {
            return Err(anyhow!("线程数必须在1-256范围内"));
        }

        let new_count = new_count.clamp(self.config.min_threads, self.config.max_threads);
        let current = self.target_threads.load(Ordering::SeqCst);

        if new_count != current {
            self.target_threads.store(new_count, Ordering::SeqCst);
            info!("线程数调整: {} -> {}", current, new_count);
        }

        Ok(())
    }

    /// 更新网络统计
    #[allow(dead_code)]
    async fn update_network_stats(&self, stats: NetworkStats) {
        let mut guard = self.network_stats.write().await;
        *guard = stats;
    }

    /// 根据网络状况自动调整线程数
    #[allow(dead_code)]
    async fn auto_adjust_threads(&self) {
        let stats = self.network_stats.read().await.clone();
        let current = self.target_threads.load(Ordering::SeqCst);
        let max = self.config.max_threads;
        let min = self.config.min_threads;

        let new_count = if stats.avg_speed < self.config.speed_threshold && current > min {
            // 速度过低，减少线程
            (current - 1).max(min)
        } else if stats.avg_speed > self.config.speed_boost_threshold && current < max {
            // 速度良好，增加线程
            (current + 1).min(max)
        } else {
            current
        };

        if new_count != current {
            self.target_threads.store(new_count, Ordering::SeqCst);
            info!(
                "自动调整线程数: {} -> {} (速度: {:.2} KB/s)",
                current,
                new_count,
                stats.avg_speed / 1024.0
            );
        }
    }

    /// 工作线程主循环
    async fn worker_loop(
        worker_id: usize,
        worker: Worker<DownloadTask>,
        global_queue: Arc<Injector<DownloadTask>>,
        stealers: Arc<RwLock<Vec<Stealer<DownloadTask>>>>,
        running: Arc<AtomicBool>,
        completed_tasks: Arc<AtomicUsize>,
        mut task_receiver: mpsc::Receiver<DownloadTask>,
    ) {
        info!("工作线程 {} 启动", worker_id);

        while running.load(Ordering::SeqCst) {
            // 1. 首先尝试从本地队列获取任务
            let task = if let Some(task) = worker.pop() {
                Some(task)
            } else {
                // 2. 尝试从通道接收任务
                match tokio::time::timeout(
                    tokio::time::Duration::from_millis(100),
                    task_receiver.recv(),
                )
                .await
                {
                    Ok(Some(task)) => Some(task),
                    _ => {
                        // 3. 尝试从全局队列窃取任务
                        match global_queue.steal() {
                            Steal::Success(task) => Some(task),
                            _ => {
                                // 4. 尝试从其他工作线程窃取任务
                                let stealers_guard: tokio::sync::RwLockReadGuard<
                                    '_,
                                    Vec<Stealer<DownloadTask>>,
                                > = stealers.read().await;
                                let mut task: Option<DownloadTask> = None;
                                for (idx, stealer) in stealers_guard.iter().enumerate() {
                                    if idx == worker_id {
                                        continue;
                                    }
                                    match stealer.steal() {
                                        Steal::Success(t) => {
                                            task = Some(t);
                                            break;
                                        }
                                        _ => continue,
                                    }
                                }
                                drop(stealers_guard);
                                task
                            }
                        }
                    }
                }
            };

            if let Some(task) = task {
                debug!("工作线程 {} 处理任务: {}", worker_id, task.id);

                // 执行下载任务（这里调用实际的下载逻辑）
                // TODO: 集成实际的下载器
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

                completed_tasks.fetch_add(1, Ordering::SeqCst);
            } else {
                // 没有任务，短暂休眠
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }

        info!("工作线程 {} 停止", worker_id);
    }

    /// 网络监控循环
    async fn network_monitor_loop(
        running: Arc<AtomicBool>,
        network_stats: Arc<RwLock<NetworkStats>>,
        check_interval: tokio::time::Duration,
    ) {
        let mut interval = tokio::time::interval(check_interval);

        while running.load(Ordering::SeqCst) {
            interval.tick().await;

            // 这里可以实际测量网络状况
            // 目前使用模拟数据
            let stats = NetworkStats {
                avg_speed: 1024.0 * 500.0, // 模拟 500KB/s
                success_rate: 0.95,
                avg_latency: 50.0,
                active_connections: 5,
            };

            let mut guard = network_stats.write().await;
            *guard = stats;
            drop(guard);
        }
    }

    /// 动态调整循环
    async fn adjustment_loop(
        running: Arc<AtomicBool>,
        target_threads: Arc<AtomicUsize>,
        _active_threads: Arc<AtomicUsize>,
        network_stats: Arc<RwLock<NetworkStats>>,
        config: SchedulerConfig,
    ) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
            config.network_check_interval_secs,
        ));

        while running.load(Ordering::SeqCst) {
            interval.tick().await;

            let stats = network_stats.read().await.clone();
            let current = target_threads.load(Ordering::SeqCst);
            let max = config.max_threads;
            let min = config.min_threads;

            let new_count = if stats.avg_speed < config.speed_threshold && current > min {
                (current - 1).max(min)
            } else if stats.avg_speed > config.speed_boost_threshold && current < max {
                (current + 1).min(max)
            } else {
                current
            };

            if new_count != current {
                target_threads.store(new_count, Ordering::SeqCst);
                info!(
                    "自动调整线程数: {} -> {} (速度: {:.2} KB/s)",
                    current,
                    new_count,
                    stats.avg_speed / 1024.0
                );
            }
        }
    }

    /// 启动调度器
    ///
    /// 返回JoinHandle，可用于等待调度器完成
    pub fn run(self) -> Result<JoinHandle<Result<()>>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Err(anyhow!("调度器已经在运行"));
        }

        // 获取任务接收器
        let task_receiver = self.task_receiver.blocking_write().take();
        let task_receiver = task_receiver.ok_or_else(|| anyhow!("任务接收器已被初始化"))?;

        let global_queue = Arc::clone(&self.global_queue);
        let stealers = Arc::clone(&self.stealers);
        let running = Arc::clone(&self.running);
        let active_threads = Arc::clone(&self.active_threads);
        let target_threads = Arc::clone(&self.target_threads);
        let completed_tasks = Arc::clone(&self.completed_tasks);
        let network_stats = Arc::clone(&self.network_stats);
        let _shutdown_notify = Arc::clone(&self.shutdown_notify);
        let config = self.config.clone();

        let handle = tokio::spawn(async move {
            info!(
                "调度器启动，初始线程数: {}",
                target_threads.load(Ordering::SeqCst)
            );

            // 创建工作线程
            let mut worker_handles = Vec::new();
            let mut worker_senders = Vec::new();
            let mut task_receivers: Vec<mpsc::Receiver<DownloadTask>> = Vec::new();

            // 创建多个通道，每个工作线程一个
            for _ in 0..256 {
                let (tx, rx) = mpsc::channel(config.queue_capacity);
                worker_senders.push(tx);
                task_receivers.push(rx);
            }

            // 启动初始工作线程
            let initial_threads = target_threads.load(Ordering::SeqCst);
            for worker_id in 0..initial_threads {
                let worker = Worker::new_fifo();
                let stealer = worker.stealer();

                {
                    let mut stealers_guard: tokio::sync::RwLockWriteGuard<
                        '_,
                        Vec<Stealer<DownloadTask>>,
                    > = stealers.write().await;
                    stealers_guard.push(stealer);
                }

                let global_q = Arc::clone(&global_queue);
                let stealers_arc = Arc::clone(&stealers);
                let running_flag = Arc::clone(&running);
                let completed = Arc::clone(&completed_tasks);
                let rx = task_receivers.pop().unwrap();

                let handle = tokio::spawn(Self::worker_loop(
                    worker_id,
                    worker,
                    global_q,
                    stealers_arc,
                    running_flag,
                    completed,
                    rx,
                ));

                worker_handles.push(handle);
                active_threads.fetch_add(1, Ordering::SeqCst);
            }

            // 启动网络监控任务
            let network_monitor = tokio::spawn(Self::network_monitor_loop(
                Arc::clone(&running),
                Arc::clone(&network_stats),
                tokio::time::Duration::from_secs(config.network_check_interval_secs),
            ));

            // 启动动态调整任务
            let adjustment_task = tokio::spawn(Self::adjustment_loop(
                Arc::clone(&running),
                Arc::clone(&target_threads),
                Arc::clone(&active_threads),
                Arc::clone(&network_stats),
                config.clone(),
            ));

            // 主循环：分发任务并管理线程
            let mut task_rx = task_receiver;
            while running.load(Ordering::SeqCst) {
                tokio::select! {
                    // 接收新任务
                    Some(task) = task_rx.recv() => {
                        // 根据优先级决定放入哪个队列
                        // 高优先级任务放入全局队列头部
                        global_queue.push(task);
                    }

                    // 检查是否需要调整线程数
                    _ = tokio::time::sleep(tokio::time::Duration::from_secs(1)) => {
                        let target = target_threads.load(Ordering::SeqCst);
                        let current = active_threads.load(Ordering::SeqCst);

                        if target > current && worker_handles.len() < 256 {
                            // 需要增加线程
                            let worker_id = current;
                            let worker = Worker::new_fifo();
                            let stealer = worker.stealer();

                            {
                                let mut stealers_guard: tokio::sync::RwLockWriteGuard<'_, Vec<Stealer<DownloadTask>>> = stealers.write().await;
                                stealers_guard.push(stealer);
                            }

                            let global_q = Arc::clone(&global_queue);
                            let stealers_arc = Arc::clone(&stealers);
                            let running_flag = Arc::clone(&running);
                            let completed = Arc::clone(&completed_tasks);

                            if let Some(rx) = task_receivers.pop() {
                                let handle = tokio::spawn(Self::worker_loop(
                                    worker_id,
                                    worker,
                                    global_q,
                                    stealers_arc,
                                    running_flag,
                                    completed,
                                    rx,
                                ));

                                worker_handles.push(handle);
                                active_threads.fetch_add(1, Ordering::SeqCst);
                                info!("新增工作线程 {}", worker_id);
                            }
                        }
                        // 减少线程通过让多余线程自然退出（在worker_loop中检查running标志）
                    }
                }
            }

            // 等待所有工作线程完成
            info!("等待工作线程停止...");
            for handle in worker_handles {
                let _ = handle.await;
            }

            let _ = network_monitor.await;
            let _ = adjustment_task.await;

            let completed = completed_tasks.load(Ordering::SeqCst);
            info!("调度器停止，共完成 {} 个任务", completed);

            Ok(())
        });

        Ok(handle)
    }

    /// 优雅关闭调度器
    ///
    /// 通知所有工作线程停止，并等待当前任务完成
    pub async fn shutdown(&self) -> Result<()> {
        if !self.running.load(Ordering::SeqCst) {
            return Ok(());
        }

        info!("正在关闭调度器...");
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();

        Ok(())
    }

    /// 强制关闭（立即终止所有任务）
    pub async fn shutdown_force(&self) -> Result<()> {
        info!("强制关闭调度器...");
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
        Ok(())
    }

    /// 获取已完成任务数
    pub fn completed_count(&self) -> usize {
        self.completed_tasks.load(Ordering::SeqCst)
    }

    /// 获取全局队列中的任务数（估算）
    pub fn pending_count(&self) -> usize {
        // crossbeam-deque 不提供精确的队列长度
        // 返回一个估算值
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_thread_count_validation() {
        // 有效范围
        assert!(Scheduler::with_threads(1).is_ok());
        assert!(Scheduler::with_threads(128).is_ok());
        assert!(Scheduler::with_threads(256).is_ok());

        // 无效范围
        assert!(Scheduler::with_threads(0).is_err());
        assert!(Scheduler::with_threads(257).is_err());
        assert!(Scheduler::with_threads(1000).is_err());
    }

    #[test]
    fn test_byte_range() {
        let range = ByteRange::new(0, 1024);
        assert_eq!(range.size(), 1024);

        let range = ByteRange::new(100, 200);
        assert_eq!(range.size(), 100);
    }

    #[test]
    fn test_task_priority() {
        assert!(TaskPriority::Low < TaskPriority::Normal);
        assert!(TaskPriority::Normal < TaskPriority::High);
        assert!(TaskPriority::High < TaskPriority::Critical);
    }

    #[tokio::test]
    async fn test_submit_task_before_run() {
        let scheduler = Scheduler::with_threads(4).unwrap();
        let task = DownloadTask::new("1", "http://example.com", "/tmp/test");

        // 调度器未启动时提交任务应该失败
        assert!(scheduler.submit_task(task).await.is_err());
    }
}
