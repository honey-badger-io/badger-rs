//! File helpers: directory sync and [`MmapFile`], a memory-mapped file that can
//! be resized. Port of the parts of ristretto's `z/file.go` that Badger uses.

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use memmap2::{Mmap, MmapMut};

use crate::error::{Error, Result, ResultExt};

/// Fsyncs a directory, so that file creations, renames and deletions in it are
/// durable. A no-op on Windows, where directories cannot be opened.
pub fn sync_dir(dir: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        File::open(dir)
            .and_then(|d| d.sync_all())
            .with_context(|| format!("while syncing directory {}", dir.display()))
    }
    #[cfg(not(unix))]
    {
        let _ = dir;
        Ok(())
    }
}

enum Map {
    Rw(MmapMut),
    Ro(Mmap),
}

/// A file mapped into memory in full (Go's `z.MmapFile`).
///
/// Writable files are mapped shared and read-write, so writes to [`data_mut`]
/// reach the file (durably after [`MmapFile::sync`]).
///
/// [`data_mut`]: MmapFile::data_mut
pub struct MmapFile {
    /// `None` only while being remapped.
    map: Option<Map>,
    file: File,
    path: PathBuf,
}

impl MmapFile {
    /// Opens `path` and maps it. With `create`, a missing file is created. If
    /// the file is empty and `max_size > 0`, it is first extended to
    /// `max_size`; the returned bool is then true ("new file", Go's
    /// `z.NewFile`).
    pub fn open(path: &Path, create: bool, read_only: bool, max_size: u64) -> Result<(Self, bool)> {
        let file = OpenOptions::new()
            .read(true)
            .write(!read_only)
            .create(create && !read_only)
            .open(path)
            .with_context(|| format!("unable to open: {}", path.display()))?;
        let mut size = file.metadata()?.len();
        let mut new = false;
        if max_size > 0 && size == 0 {
            if read_only {
                return Err(Error::corruption(format!(
                    "{} is empty and cannot be extended read-only",
                    path.display()
                )));
            }
            file.set_len(max_size).context("error while truncation")?;
            size = max_size;
            new = true;
        }
        let map = Self::map(&file, read_only)
            .with_context(|| format!("while mmapping {} with size: {size}", path.display()))?;
        if size == 0
            && let Some(dir) = path.parent()
        {
            sync_dir(dir)?;
        }
        Ok((
            MmapFile {
                map: Some(map),
                file,
                path: path.to_path_buf(),
            },
            new,
        ))
    }

    /// Maps the whole file.
    ///
    /// Soundness relies on Badger owning the files in its directory: while
    /// mapped, a file is only changed through this mapping, or resized by
    /// `truncate` after the mapping is dropped. Other processes are kept out
    /// by the directory lock, and the DB never maps one file twice (a second
    /// `MmapFile` truncating it would leave the first one's pages beyond the
    /// end of the file).
    fn map(file: &File, read_only: bool) -> std::io::Result<Map> {
        Ok(if read_only {
            // SAFETY: see above.
            Map::Ro(unsafe { Mmap::map(file) }?)
        } else {
            // SAFETY: see above.
            Map::Rw(unsafe { MmapMut::map_mut(file) }?)
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn file(&self) -> &File {
        &self.file
    }

    pub fn is_read_only(&self) -> bool {
        matches!(self.map, Some(Map::Ro(_)))
    }

    /// The mapped bytes: the whole file.
    pub fn data(&self) -> &[u8] {
        match &self.map {
            Some(Map::Rw(m)) => m,
            Some(Map::Ro(m)) => m,
            None => &[],
        }
    }

    /// The mapped bytes, writable.
    ///
    /// # Panics
    /// If the file was opened read-only.
    pub fn data_mut(&mut self) -> &mut [u8] {
        match &mut self.map {
            Some(Map::Rw(m)) => m,
            Some(Map::Ro(_)) => panic!("{} is mapped read-only", self.path.display()),
            None => &mut [],
        }
    }

    /// Flushes the mapping to disk (msync).
    pub fn sync(&self) -> Result<()> {
        match &self.map {
            Some(Map::Rw(m)) => m
                .flush()
                .with_context(|| format!("while sync file: {}", self.path.display())),
            _ => Ok(()),
        }
    }

    /// Resizes the file to `size` and remaps it.
    pub fn truncate(&mut self, size: u64) -> Result<()> {
        self.sync()?;
        let read_only = self.is_read_only();
        // Unmap first: some platforms refuse to shrink a mapped file.
        self.map = None;
        self.file
            .set_len(size)
            .with_context(|| format!("while truncate file: {}", self.path.display()))?;
        self.map = Some(Self::map(&self.file, read_only)?);
        Ok(())
    }

    /// Unmaps, empties and removes the file (Go's `Delete`).
    pub fn delete(self) -> Result<()> {
        let MmapFile { map, file, path } = self;
        drop(map);
        file.set_len(0)
            .with_context(|| format!("while truncate file: {}", path.display()))?;
        drop(file);
        std::fs::remove_file(&path).with_context(|| format!("while removing {}", path.display()))
    }

    /// Like [`MmapFile::delete`], for a file other threads may still hold:
    /// the mapping is dropped (later reads see no data) and the file removed.
    pub fn delete_in_place(&mut self) -> Result<()> {
        self.map = None;
        self.file
            .set_len(0)
            .with_context(|| format!("while truncate file: {}", self.path.display()))?;
        std::fs::remove_file(&self.path)
            .with_context(|| format!("while removing {}", self.path.display()))
    }

    /// Syncs, unmaps and closes the file, first resizing it to `max_size` if
    /// given (Go's `Close`).
    pub fn close(self, max_size: Option<u64>) -> Result<()> {
        self.sync()?;
        let MmapFile { map, file, path } = self;
        drop(map);
        if let Some(size) = max_size {
            file.set_len(size)
                .with_context(|| format!("while truncate file: {}", path.display()))?;
        }
        Ok(())
    }
}
