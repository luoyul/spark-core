//! Indicatif 进度报告器
//!
//! 使用 indicatif 库的 ProgressBar 实现进度报告。

use indicatif::{ProgressBar, ProgressStyle};

use super::reporter::{ProgressEvent, ProgressReporter};

/// 基于 indicatif::ProgressBar 的进度报告器
pub struct IndicatifReporter {
    pb: ProgressBar,
}

impl IndicatifReporter {
    /// 创建新的 indicatif 报告器
    pub fn new() -> Self {
        let pb = ProgressBar::new(0);
        Self { pb }
    }

    /// 自定义模板创建
    pub fn with_template(template: &str) -> Option<Self> {
        let pb = ProgressBar::new(0);
        let style = ProgressStyle::default_bar()
            .template(template)
            .ok()?
            .progress_chars("#>-");
        pb.set_style(style);
        Some(Self { pb })
    }
}

impl Default for IndicatifReporter {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressReporter for IndicatifReporter {
    fn on_progress(&self, event: &ProgressEvent) -> bool {
        // 首次进度更新时设置总长度
        if self.pb.length().unwrap_or(0) == 0 && event.total_bytes > 0 {
            self.pb.set_length(event.total_bytes);
            let style = ProgressStyle::default_bar()
                .template(
                    "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
                     {bytes}/{total_bytes} ({eta})",
                )
                .unwrap()
                .progress_chars("#>-");
            self.pb.set_style(style);
        }

        let current = self.pb.position();
        if event.downloaded_bytes > current {
            self.pb.set_position(event.downloaded_bytes);
        }

        true
    }

    fn on_start(&self, total_bytes: u64, _total_chunks: usize) {
        self.pb.set_length(total_bytes);
        let style = ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] \
                 {bytes}/{total_bytes} ({eta})",
            )
            .unwrap()
            .progress_chars("#>-");
        self.pb.set_style(style);
    }

    fn on_complete(&self) {
        self.pb.finish_with_message("Done");
    }

    fn on_error(&self, error: &str) {
        self.pb.finish_with_message(format!("Error: {}", error));
    }
}

impl Drop for IndicatifReporter {
    fn drop(&mut self) {
        // 确保进度条被正确关闭
        if !self.pb.is_finished() {
            self.pb.finish();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_indicatif_reporter_creation() {
        let reporter = IndicatifReporter::new();
        // 不 panic 即通过
        let _ = reporter;
    }

    #[test]
    fn test_indicatif_reporter_drop_finishes() {
        let reporter = IndicatifReporter::new();
        reporter.on_start(1024, 4);
        // Drop 时应自动 finish
        drop(reporter);
    }

    #[test]
    fn test_indicatif_reporter_on_error() {
        let reporter = IndicatifReporter::new();
        reporter.on_error("connection timeout");
        // 不 panic 即通过
    }
}
