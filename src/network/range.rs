//! HTTP Range请求处理
//!
//! 支持分块下载的范围计算

use thiserror::Error;

/// Range解析错误
#[derive(Debug, Error)]
pub enum RangeError {
    #[error("Invalid range format: {0}")]
    InvalidFormat(String),
    #[error("Invalid range values: start={start}, end={end}")]
    InvalidValues { start: u64, end: u64 },
    #[error("Range start must be <= end")]
    StartGreaterThanEnd,
}

/// HTTP字节范围
///
/// 表示一个字节范围 [start, end)，包含start，不包含end
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Range {
    /// 起始位置（包含）
    pub start: u64,
    /// 结束位置（不包含）
    pub end: u64,
}

impl Range {
    /// 创建新的Range
    ///
    /// # Arguments
    /// * `start` - 起始字节位置（包含）
    /// * `end` - 结束字节位置（不包含）
    ///
    /// # Errors
    /// 如果start > end，返回错误
    pub fn new(start: u64, end: u64) -> Result<Self, RangeError> {
        if start > end {
            return Err(RangeError::StartGreaterThanEnd);
        }
        Ok(Self { start, end })
    }

    /// 返回范围长度（字节数）
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// 检查范围是否为空
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// 生成HTTP Range请求头格式 "bytes=start-end"
    ///
    /// 注意：HTTP Range头使用闭区间 [start, end]，所以end需要减1
    pub fn to_header_string(&self) -> String {
        if self.is_empty() {
            format!("bytes={}-", self.start)
        } else {
            // HTTP Range使用闭区间，end-1
            format!("bytes={}-{}", self.start, self.end.saturating_sub(1))
        }
    }

    /// 解析Content-Range响应头
    ///
    /// 格式: "bytes start-end/total" 或 "bytes start-end/*"
    ///
    /// # Arguments
    /// * `header` - Content-Range头值
    ///
    /// # Returns
    /// 成功返回 (Range, total_size)，total_size为None时表示未知(*)
    pub fn parse_header(header: &str) -> Result<(Self, Option<u64>), RangeError> {
        let header = header.trim();

        // 必须以 "bytes " 开头
        if !header.starts_with("bytes ") {
            return Err(RangeError::InvalidFormat(header.to_string()));
        }

        let content = &header[6..]; // 去掉 "bytes " 前缀

        // 分割范围部分和总大小部分
        let parts: Vec<&str> = content.split('/').collect();
        if parts.len() != 2 {
            return Err(RangeError::InvalidFormat(header.to_string()));
        }

        // 解析范围部分 "start-end"
        let range_part = parts[0].trim();
        let range_values: Vec<&str> = range_part.split('-').collect();
        if range_values.len() != 2 {
            return Err(RangeError::InvalidFormat(header.to_string()));
        }

        let start: u64 = range_values[0]
            .trim()
            .parse()
            .map_err(|_| RangeError::InvalidFormat(header.to_string()))?;

        let end: u64 = range_values[1]
            .trim()
            .parse()
            .map_err(|_| RangeError::InvalidFormat(header.to_string()))?;

        // 验证范围值
        if start > end {
            return Err(RangeError::InvalidValues { start, end });
        }

        // 解析总大小
        let total_size = if parts[1].trim() == "*" {
            None
        } else {
            Some(
                parts[1]
                    .trim()
                    .parse::<u64>()
                    .map_err(|_| RangeError::InvalidFormat(header.to_string()))?,
            )
        };

        // 注意：Content-Range的end是包含的，需要+1转为不包含
        let range = Range::new(start, end + 1)?;

        Ok((range, total_size))
    }

    /// 检查两个范围是否重叠
    pub fn overlaps(&self, other: &Range) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// 合并两个重叠的范围
    ///
    /// # Returns
    /// 如果范围重叠，返回合并后的范围；否则返回None
    pub fn merge(&self, other: &Range) -> Option<Range> {
        if self.overlaps(other) {
            Some(Range {
                start: self.start.min(other.start),
                end: self.end.max(other.end),
            })
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_new() {
        let range = Range::new(0, 100).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 100);
        assert_eq!(range.len(), 100);
    }

    #[test]
    fn test_range_new_invalid() {
        assert!(Range::new(100, 0).is_err());
    }

    #[test]
    fn test_to_header_string() {
        let range = Range::new(0, 100).unwrap();
        assert_eq!(range.to_header_string(), "bytes=0-99");

        let range = Range::new(1000, 2000).unwrap();
        assert_eq!(range.to_header_string(), "bytes=1000-1999");
    }

    #[test]
    fn test_parse_header() {
        let (range, total) = Range::parse_header("bytes 0-99/1000").unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 100);
        assert_eq!(range.len(), 100);
        assert_eq!(total, Some(1000));

        let (range, total) = Range::parse_header("bytes 100-199/*").unwrap();
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 200);
        assert_eq!(total, None);
    }

    #[test]
    fn test_parse_header_invalid() {
        assert!(Range::parse_header("invalid").is_err());
        assert!(Range::parse_header("bytes 100/1000").is_err());
        assert!(Range::parse_header("bytes 100-50/1000").is_err());
    }

    #[test]
    fn test_range_overlap() {
        let r1 = Range::new(0, 100).unwrap();
        let r2 = Range::new(50, 150).unwrap();
        assert!(r1.overlaps(&r2));

        let r3 = Range::new(100, 200).unwrap();
        assert!(!r1.overlaps(&r3));
    }
}
