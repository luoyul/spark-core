//! 稀疏文件处理模块
//!
//! 支持跨平台稀疏文件的创建、读写和打洞操作，有效节省磁盘空间。
//! 使用条件编译区分不同平台的实现。

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

/// 稀疏文件处理器
///
/// 提供跨平台的稀疏文件操作能力，包括创建、读写、打洞和预分配等功能。
pub struct SparseFile {
    file: File,
    path: std::path::PathBuf,
}

impl SparseFile {
    /// 创建新的稀疏文件
    ///
    /// # Arguments
    /// * `path` - 文件路径
    /// * `size` - 文件大小（字节）
    ///
    /// # Returns
    /// 返回创建好的SparseFile实例
    ///
    /// # Platform Notes
    /// - Windows: 使用FSCTL_SET_SPARSE将文件标记为稀疏
    /// - Linux/macOS: 使用fallocate或fcntl创建稀疏文件
    pub fn create<P: AsRef<Path>>(path: P, size: u64) -> Result<Self> {
        let path = path.as_ref().to_path_buf();

        // 创建文件并设置大小
        let file =
            File::create(&path).with_context(|| format!("Failed to create file at {:?}", path))?;

        let mut sparse_file = Self { file, path };

        // 将文件标记为稀疏文件（平台特定）
        sparse_file.mark_sparse()?;

        // 预分配文件大小
        sparse_file.preallocate(size)?;

        Ok(sparse_file)
    }

    /// 打开已有的稀疏文件
    ///
    /// # Arguments
    /// * `path` - 文件路径
    ///
    /// # Returns
    /// 返回打开的SparseFile实例
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::options()
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("Failed to open file at {:?}", path))?;

        Ok(Self { file, path })
    }

    /// 在指定位置写入数据
    ///
    /// # Arguments
    /// * `offset` - 写入位置偏移量（字节）
    /// * `data` - 要写入的数据
    ///
    /// # Returns
    /// 返回实际写入的字节数
    pub fn write_at(&mut self, offset: u64, data: &[u8]) -> Result<usize> {
        self.file
            .seek(SeekFrom::Start(offset))
            .context("Failed to seek to offset")?;
        let written = self.file.write(data).context("Failed to write data")?;
        self.file.sync_data().context("Failed to sync data")?;
        Ok(written)
    }

    /// 从指定位置读取数据
    ///
    /// # Arguments
    /// * `offset` - 读取位置偏移量（字节）
    /// * `buffer` - 用于存储读取数据的缓冲区
    ///
    /// # Returns
    /// 返回实际读取的字节数
    pub fn read_at(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        self.file
            .seek(SeekFrom::Start(offset))
            .context("Failed to seek to offset")?;
        let read = self.file.read(buffer).context("Failed to read data")?;
        Ok(read)
    }

    /// 预分配文件大小但不写入数据
    ///
    /// # Arguments
    /// * `size` - 要预分配的文件大小（字节）
    ///
    /// # Platform Notes
    /// - Windows: 使用SetFileValidData或SetFilePointerEx + SetEndOfFile
    /// - Linux: 使用fallocate
    /// - macOS: 使用fcntl(F_PREALLOCATE)或ftruncate
    pub fn preallocate(&mut self, size: u64) -> Result<()> {
        self.preallocate_platform(size)
    }

    /// 打洞操作 - 释放已分配的磁盘空间
    ///
    /// # Arguments
    /// * `offset` - 打洞起始位置（字节）
    /// * `len` - 打洞长度（字节）
    ///
    /// # Platform Notes
    /// - Windows: 使用FSCTL_SET_ZERO_DATA
    /// - Linux: 使用fallocate(FALLOC_FL_PUNCH_HOLE)
    /// - macOS: 使用fcntl(F_PUNCHHOLE)（如果支持）
    pub fn punch_hole(&mut self, offset: u64, len: u64) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        self.punch_hole_platform(offset, len)
    }

    /// 获取文件路径
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// 获取文件大小
    pub fn size(&self) -> Result<u64> {
        let metadata = self
            .file
            .metadata()
            .context("Failed to get file metadata")?;
        Ok(metadata.len())
    }

    /// 刷新文件数据到磁盘
    pub fn sync(&mut self) -> Result<()> {
        self.file.sync_all().context("Failed to sync file")
    }

    /// 将文件标记为稀疏文件（平台特定实现）
    #[cfg(windows)]
    fn mark_sparse(&mut self) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        let handle = self.file.as_raw_handle();
        let mut bytes_returned: u32 = 0;

        // FSCTL_SET_SPARSE = 0x900c4
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
            // 获取错误码
            let error = unsafe { windows_sys::Win32::Foundation::GetLastError() };
            // ERROR_INVALID_FUNCTION (1) 表示文件系统不支持稀疏文件
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
        // Linux/macOS不需要显式标记稀疏文件
        // 文件系统会根据写入模式自动处理
        Ok(())
    }

    /// Windows平台预分配实现
    #[cfg(windows)]
    fn preallocate_platform(&mut self, size: u64) -> Result<()> {
        use std::os::windows::io::AsRawHandle;

        let handle = self.file.as_raw_handle();

        // 设置文件指针到目标大小
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

        // 设置文件结束位置
        let result = unsafe { windows_sys::Win32::Storage::FileSystem::SetEndOfFile(handle as _) };

        if result == 0 {
            return Err(anyhow::anyhow!("Failed to set end of file"));
        }

        Ok(())
    }

    /// Linux/macOS平台预分配实现
    #[cfg(unix)]
    fn preallocate_platform(&mut self, size: u64) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();

        #[cfg(target_os = "linux")]
        {
            // Linux: 使用fallocate进行预分配
            let result = unsafe { libc::fallocate(fd, 0, 0, size as libc::off_t) };

            if result == 0 {
                return Ok(());
            }

            // fallocate失败时回退到ftruncate
            tracing::warn!("fallocate failed, falling back to ftruncate");
        }

        #[cfg(target_os = "macos")]
        {
            // macOS: 使用fcntl(F_PREALLOCATE)进行预分配
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

        // 回退方案：使用ftruncate
        let result = unsafe { libc::ftruncate(fd, size as libc::off_t) };

        if result == -1 {
            return Err(anyhow::anyhow!(
                "Failed to preallocate file: {}",
                std::io::Error::last_os_error()
            ));
        }

        Ok(())
    }

    /// Windows平台打洞实现
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

        // FSCTL_SET_ZERO_DATA = 0x980c8
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
                offset,
                len,
                error
            ));
        }

        Ok(())
    }

    /// Linux平台打洞实现
    #[cfg(target_os = "linux")]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();

        // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE = 0x03
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
                offset,
                len,
                std::io::Error::last_os_error()
            ));
        }

        Ok(())
    }

    /// macOS平台打洞实现
    #[cfg(target_os = "macos")]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let fd = self.file.as_raw_fd();

        // macOS 10.14+ 支持 F_PUNCHHOLE
        // 如果不支持，则使用 SEEK_HOLE/SEEK_DATA 或回退到写入零
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

        // 回退方案：写入零并建议系统释放（不保证有效）
        tracing::warn!(
            "Punch hole not fully supported on this macOS version, falling back to zero write"
        );

        // 写入零字节
        let zeros = vec![0u8; std::cmp::min(len as usize, 1024 * 1024)];
        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let to_write = std::cmp::min(remaining as usize, zeros.len());
            self.write_at(current_offset, &zeros[..to_write])?;
            remaining -= to_write as u64;
            current_offset += to_write as u64;
        }

        // 尝试使用F_TRIM_ACTIVE_FILE（如果支持）
        unsafe {
            libc::fcntl(fd, libc::F_FULLFSYNC, 0);
        }

        Ok(())
    }

    /// 通用Unix平台打洞实现（用于非Linux非macOS）
    #[cfg(all(unix, not(target_os = "linux"), not(target_os = "macos")))]
    fn punch_hole_platform(&mut self, offset: u64, len: u64) -> Result<()> {
        // 通用回退方案：写入零
        tracing::warn!("Punch hole not supported on this platform, falling back to zero write");

        let zeros = vec![0u8; std::cmp::min(len as usize, 1024 * 1024)];
        let mut remaining = len;
        let mut current_offset = offset;

        while remaining > 0 {
            let to_write = std::cmp::min(remaining as usize, zeros.len());
            self.write_at(current_offset, &zeros[..to_write])?;
            remaining -= to_write as u64;
            current_offset += to_write as u64;
        }

        Ok(())
    }
}

impl Drop for SparseFile {
    fn drop(&mut self) {
        // 确保数据写入磁盘
        let _ = self.file.sync_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_create_sparse_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_sparse.bin");

        let sparse = SparseFile::create(&path, 1024 * 1024);
        assert!(sparse.is_ok());

        let mut file = sparse.unwrap();
        let size = file.size().unwrap();
        assert_eq!(size, 1024 * 1024);
    }

    #[test]
    fn test_write_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_rw.bin");

        let mut file = SparseFile::create(&path, 1024 * 1024).unwrap();

        // 写入数据
        let data = b"Hello, Sparse File!";
        let written = file.write_at(100, data).unwrap();
        assert_eq!(written, data.len());

        // 读取数据
        let mut buffer = vec![0u8; data.len()];
        let read = file.read_at(100, &mut buffer).unwrap();
        assert_eq!(read, data.len());
        assert_eq!(&buffer, data);
    }

    #[test]
    fn test_punch_hole() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_hole.bin");

        let mut file = SparseFile::create(&path, 1024 * 1024).unwrap();

        // 先写入一些数据
        let data = vec![0xABu8; 4096];
        file.write_at(0, &data).unwrap();
        file.write_at(8192, &data).unwrap();

        // 打洞释放空间
        let result = file.punch_hole(4096, 4096);
        // 打洞可能失败（如果文件系统不支持），但不应该panic
        if result.is_err() {
            println!("Punch hole not supported or failed: {:?}", result);
        }
    }

    #[test]
    fn test_open_existing() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test_open.bin");

        // 创建文件
        {
            let mut file = SparseFile::create(&path, 1024 * 1024).unwrap();
            file.write_at(100, b"test data").unwrap();
        }

        // 打开文件
        let mut file = SparseFile::open(&path).unwrap();
        let mut buffer = vec![0u8; 9];
        let read = file.read_at(100, &mut buffer).unwrap();
        assert_eq!(read, 9);
        assert_eq!(&buffer, b"test data");
    }
}
