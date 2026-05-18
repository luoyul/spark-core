//! 统一 HTTP 字节范围类型
//!
//! 表示半开区间 [start, end)，合并了原有 3 个重复的 Range 定义

use thiserror::Error;

/// Range 错误
#[derive(Debug, Error)]
pub enum RangeError {
    #[error("范围起始位置不能大于结束位置: start={start}, end={end}")]
    StartAfterEnd { start: u64, end: u64 },

    #[error("无效的范围格式: {0}")]
    InvalidFormat(String),
}

/// HTTP 字节范围 [start, end)，半开区间
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct HttpRange {
    pub start: u64,
    pub end: u64,
}

impl HttpRange {
    /// 创建带验证的范围
    pub fn new(start: u64, end: u64) -> Result<Self, RangeError> {
        if start > end {
            return Err(RangeError::StartAfterEnd { start, end });
        }
        Ok(Self { start, end })
    }

    /// 创建不带验证的范围
    pub fn new_unchecked(start: u64, end: u64) -> Self {
        Self { start, end }
    }

    /// 返回范围长度（字节数）
    pub fn len(&self) -> u64 {
        self.end.saturating_sub(self.start)
    }

    /// 检查范围是否为空
    pub fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    /// size() — len() 的别名，向后兼容 scheduler::ByteRange 和 task::Range
    pub fn size(&self) -> u64 {
        self.len()
    }

    /// 生成 HTTP Range 请求头格式 "bytes=start-end"
    pub fn to_http_header(&self) -> String {
        if self.is_empty() {
            format!("bytes={}-", self.start)
        } else {
            format!("bytes={}-{}", self.start, self.end.saturating_sub(1))
        }
    }

    /// to_http_range() — to_http_header() 的别名，向后兼容 task::Range
    pub fn to_http_range(&self) -> String {
        self.to_http_header()
    }

    /// 解析 Content-Range 响应头
    pub fn parse_content_range_header(header: &str) -> Result<(Self, Option<u64>), RangeError> {
        let header = header.trim();

        if !header.starts_with("bytes ") {
            return Err(RangeError::InvalidFormat(header.to_string()));
        }

        let content = &header[6..];

        let parts: Vec<&str> = content.split('/').collect();
        if parts.len() != 2 {
            return Err(RangeError::InvalidFormat(header.to_string()));
        }

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

        if start > end {
            return Err(RangeError::StartAfterEnd { start, end });
        }

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

        let range = HttpRange::new(start, end + 1)?;
        Ok((range, total_size))
    }

    /// 检查两个范围是否重叠
    pub fn overlaps(&self, other: &HttpRange) -> bool {
        self.start < other.end && other.start < self.end
    }

    /// 合并两个重叠的范围
    pub fn merge(&self, other: &HttpRange) -> Option<HttpRange> {
        if self.overlaps(other) {
            Some(HttpRange {
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
        let range = HttpRange::new(0, 100).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 100);
        assert_eq!(range.len(), 100);
        assert_eq!(range.size(), 100);
    }

    #[test]
    fn test_range_new_invalid() {
        assert!(HttpRange::new(100, 0).is_err());
    }

    #[test]
    fn test_new_unchecked() {
        let range = HttpRange::new_unchecked(100, 0);
        assert!(range.is_empty());
    }

    #[test]
    fn test_to_http_header() {
        let range = HttpRange::new(0, 100).unwrap();
        assert_eq!(range.to_http_header(), "bytes=0-99");
        assert_eq!(range.to_http_range(), "bytes=0-99");

        let range = HttpRange::new(1000, 2000).unwrap();
        assert_eq!(range.to_http_header(), "bytes=1000-1999");
    }

    #[test]
    fn test_parse_header() {
        let (range, total) = HttpRange::parse_content_range_header("bytes 0-99/1000").unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.end, 100);
        assert_eq!(range.len(), 100);
        assert_eq!(total, Some(1000));

        let (range, total) = HttpRange::parse_content_range_header("bytes 100-199/*").unwrap();
        assert_eq!(range.start, 100);
        assert_eq!(range.end, 200);
        assert_eq!(total, None);
    }

    #[test]
    fn test_parse_header_invalid() {
        assert!(HttpRange::parse_content_range_header("invalid").is_err());
        assert!(HttpRange::parse_content_range_header("bytes 100/1000").is_err());
        assert!(HttpRange::parse_content_range_header("bytes 100-50/1000").is_err());
    }

    #[test]
    fn test_range_overlap() {
        let r1 = HttpRange::new(0, 100).unwrap();
        let r2 = HttpRange::new(50, 150).unwrap();
        assert!(r1.overlaps(&r2));

        let r3 = HttpRange::new(100, 200).unwrap();
        assert!(!r1.overlaps(&r3));
    }

    #[test]
    fn test_range_merge() {
        let r1 = HttpRange::new(0, 100).unwrap();
        let r2 = HttpRange::new(50, 150).unwrap();
        let merged = r1.merge(&r2).unwrap();
        assert_eq!(merged.start, 0);
        assert_eq!(merged.end, 150);

        let r3 = HttpRange::new(200, 300).unwrap();
        assert!(r1.merge(&r3).is_none());
    }
}
