//! 存储后端抽象层
//!
//! 提供统一的存储后端 trait，支持稀疏文件、内存等多种后端实现。

pub mod backend;
pub mod sparse_file;
pub mod memory;
