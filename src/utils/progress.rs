//! 进度显示
//!
//! 使用indicatif显示下载进度

use indicatif::{ProgressBar, ProgressStyle};

/// 进度条包装器
pub struct ProgressReporter {
    pb: ProgressBar,
}

impl ProgressReporter {
    /// 创建新的进度报告器
    pub fn new(total: u64) -> Self {
        let pb = ProgressBar::new(total);
        let style = ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .unwrap()
            .progress_chars("#>-");
        pb.set_style(style);
        Self { pb }
    }

    /// 更新进度
    pub fn inc(&self, delta: u64) {
        self.pb.inc(delta);
    }

    /// 完成
    pub fn finish(&self) {
        self.pb.finish_with_message("Done");
    }
}
