//! 稀疏文件存储后端
//!
//! 基于稀疏文件的存储实现，支持跨平台预分配、打洞、稀疏标记。
//! 实现 StorageBackend trait。

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use super::backend::StorageBackend;

/// 稀疏文件后端
///
/// 封装跨平台稀疏文件操作，实现 StorageBackend trait。
/// Windows 使用 FSCTL_SET_SPARSE/FSCTL_SET_ZERO_DATA，
/// Linux 使用 fallocate，macOS 使用 fcntl。
pub struct SparseFileBackend {
    file: File,
    path: PathBuf,
}

impl SparseFileBackend {
    /// 打开已有的稀疏文件
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("Failed to open file at {:?}", path))?;

        Ok(Self { file, path })
    }

    /// 从已打开的 File 和路径构造（由调用方创建文件）
    pub fn from_file(file: File, path: PathBuf) -> Self {
        Self { file, path }
    }

    /// 在指定偏移量写入数据（同步版本）
    fn write_at_sync(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        self.file
            .seek(SeekFrom::Start(offset))
            .context("Failed to seek to offset")?;
        let written = self.file.write(data).context("Failed to write data")?;
        self.file.sync_data().context("Failed to sync data")?;
        Ok(written)
    }

    /// 从指定偏移量读取数据（同步版本）
    fn read_at_sync(&self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        let mut file = &self.file;
        file.seek(SeekFrom::Start(offset))
            .context("Failed to seek to offset")?;
        let read = file.read(buffer).context("Failed to read data")?;
        Ok(read)
    }

    /// 预分配文件大小但不写入数据
    pub fn preallocate(&mut self, size: u64) -> Result<()> {
        self.preallocate_platform(size)
    }

    /// 打洞操作 - 释放已分配的磁盘空间
    pub fn punch_hole(&mut self, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        self.punch_hole_platform(offset, len)
    }

    /// 获取文件路径
    pub fn file_path(&self) -> &Path {
        &self.path
    }

    /// 获取文件大小
    pub fn size(&self) -> Result<u64> {
        let metadata = self.file.metadata().context("Failed to get file metadata")?;
        Ok(metadata.len())
    }

    /// 将文件标记为稀疏文件（平台特定实现）
    #[cfg(windows)]
    fn mark_sparse(&mut self) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        let handle = self.file.as_raw_handle();
        let mut bytes_returned: u32 = 0;

        const FSCTL_SET_SPARSE: u32 = 0x900c4;

        let result = unsafe {
            windows_sys::Win32::System::IO::DeviceIoControl(
                handle as _,
                FSCTL_SET_SPARSE,
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                0,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };

        if result == 0 {
            let error = unsafe { windows_sys::Win32::Foundation::GetLastError() };
            if error == 1 {
                tracing::warn!("File system does not support sparse files");
                return Ok(());
            }
            return Err(anyhow::anyhow!(
                "Failed to mark file as sparse, error: {}",
                error
            ));
        }

        Ok(())
    }

    #[cfg(unix)]
    fn mark_sparse(&mut self) -> Result<()> {
        // Linux/macOS 不需要显式标记稀疏文件
        Ok(())
    }

    /// Windows 平台预分配实现
    #[cfg(windows)]
    fn preallocate_platform(&mut self, size: u64) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        let handle = self.file.as_raw_handle();
        let distance = size as i64;
        let result = unsafe {
            windows_sys::Win32::Storage::FileSystem::SetFilePointerEx(
                handle as _,
                distance,
                std::ptr::null_mut(),
                windows_sys::Win32::Storage::FileSystem::FILE_BEGIN,
            )
        };

        if result == 0 {
            return Err(anyhow::anyhow!("Failed to set file pointer"));
        }

        let result = unsafe { windows_sys::Win32::Storage::FileSystem::SetEndOfFile(handle as _) };

        if result == 0 {
            return Err(anyhow::anyhow!("Failed to set end of file"));
        }

        Ok(())
    }

    /// Linux/macOS 平台预分配实现
    #[cfg(unix)]
    fn preallocate_platform(&mut self, size: u64) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();

        #[cfg(target_os = "linux")]
        {
            let result = unsafe { libc::fallocate(fd, 0, 0, size as libc::off_t) };
            if result == 0 {
                return Ok(());
            }
            tracing::warn!("fallocate failed, falling back to ftruncate");
        }

        #[cfg(target_os = "macos")]
        {
            let store = libc::fstore_t {
                fst_flags: libc::F_ALLOCATECONTIG,
                fst_posmode: libc::F_PEOFPOSMODE,
                fst_offset: 0,
                fst_length: size as libc::off_t,
                fst_bytesalloc: 0,
            };

            let result = unsafe { libc::fcntl(fd, libc::F_PREALLOCATE, &store) };
            if result == -1 {
                tracing::warn!("F_PREALLOCATE failed, falling back to ftruncate");
            }
        }

        let result = unsafe { libc::ftruncate(fd, size as libc::off_t) };
        if result == -1 {
            return Err(anyhow::anyhow!(
                "Failed to preallocate file: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(())
    }

    /// Windows 平台打洞实现
    #[cfg(windows)]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        #[repr(C)]
        struct FILE_ZERO_DATA_INFORMATION {
            file_offset: i64,
            beyond_final_zero: i64,
        }

        let handle = self.file.as_raw_handle();
        let zero_data = FILE_ZERO_DATA_INFORMATION {
            file_offset: offset as i64,
            beyond_final_zero: (offset + len) as i64,
        };

        let mut bytes_returned: u32 = 0;
        const FSCTL_SET_ZERO_DATA: u32 = 0x980c8;

        let result = unsafe {
            windows_sys::Win32::System::IO::DeviceIoControl(
                handle as _,
                FSCTL_SET_ZERO_DATA,
                &zero_data as *const _ as *const std::ffi::c_void,
                std::mem::size_of::<FILE_ZERO_DATA_INFORMATION>() as u32,
                std::ptr::null_mut(),
                0,
                &mut bytes_returned,
                std::ptr::null_mut(),
            )
        };

        if result == 0 {
            let error = unsafe { windows_sys::Win32::Foundation::GetLastError() };
            return Err(anyhow::anyhow!(
                "Failed to punch hole at offset {} with length {}, error: {}",
                offset, len, error
            ));
        }

        Ok(())
    }

    /// Linux 平台打洞实现
    #[cfg(target_os = "linux")]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();

        const FALLOC_FL_PUNCH_HOLE: i32 = 0x02;
        const FALLOC_FL_KEEP_SIZE: i32 = 0x01;

        let result = unsafe {
            libc::fallocate(
                fd,
                FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                offset as libc::off_t,
                len as libc::off_t,
            )
        };

        if result == -1 {
            return Err(anyhow::anyhow!(
                "Failed to punch hole at offset {} with length {}: {}",
                offset, len,
                std::io::Error::last_os_error()
            ));
        }

        Ok(())
    }

    /// macOS 平台打洞实现
    #[cfg(target_os = "macos")]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();

        #[cfg(feature = "macos_punchhole")]
        {
            let punchhole = libc::fpunchhole_t {
                fp_flags: 0,
                reserved: 0,
                fp_offset: offset as libc::off_t,
                fp_length: len as libc::off_t,
            };

            let result = unsafe { libc::fcntl(fd, libc::F_PUNCHHOLE, &punchhole) };
            if result == 0 {
                return Ok(());
            }
        }

        tracing::warn!("Punch hole not fully supported on this macOS version, falling back to zero write");

        let zeros = vec![0u8; std::cmp::min(len as usize, 1024 * 1024)];
        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let to_write = std::cmp::min(remaining as usize, zeros.len());
            self.write_at_sync(current_offset, &zeros[..to_write])?;
            remaining -= to_write as u64;
            current_offset += to_write as u64;
        }

        unsafe {
            libc::fcntl(fd, libc::F_FULLFSYNC, 0);
        }

        Ok(())
    }

    /// 通用 Unix 平台打洞实现（非 Linux 非 macOS）
    #[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        tracing::warn!("Punch hole not supported on this platform, falling back to zero write");

        let zeros = vec![0u8; std::cmp::min(len as usize, 1024 * 1024)];
        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let to_write = std::cmp::min(remaining as usize, zeros.len());
            self.write_at_sync(current_offset, &zeros[..to_write])?;
            remaining -= to_write as u64;
            current_offset += to_write as u64;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl StorageBackend for SparseFileBackend {
    async fn create(&mut self, file_size: u64) -> Result<()> {
        self.mark_sparse()?;
        self.preallocate(file_size)?;
        Ok(())
    }

    async fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        self.write_at_sync(offset, data)
    }

    async fn read_at(&self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        self.read_at_sync(offset, buf)
    }

    fn completed_ranges(&self) -> Vec<(u64, u64)> {
        // Phase 4 实现区间追踪
        Vec::new()
    }

    async fn flush(&mut self) -> Result<()> {
        self.file.sync_all().context("Failed to sync file")
    }

    fn path(&self) -> Option<&Path> {
        Some(&self.path)
    }
}

impl Drop for SparseFileBackend {
    fn drop(&mut self) {
        let _ = self.file.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sparse_file_backend_create_and_write() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sparse.bin");

        // SparseFileBackend::create 期望文件已存在并有 handle
        // 先创建文件
        let file = File::create(&path).unwrap();
        let mut backend = SparseFileBackend {
            file,
            path: path.clone(),
        };

        backend.create(1024 * 1024).await.unwrap();

        let data = b"Hello, Sparse File!";
        let written = backend.write_at(100, data).await.unwrap();
        assert_eq!(written, data.len());

        let mut buf = vec![0u8; data.len()];
        let read = backend.read_at(100, &mut buf).await.unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buf, data);
    }

    #[tokio::test]
    async fn test_sparse_file_backend_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_path.bin");

        let file = File::create(&path).unwrap();
        let backend = SparseFileBackend {
            file,
            path: path.clone(),
        };

        assert_eq!(backend.path(), Some(path.as_path()));
    }
}
