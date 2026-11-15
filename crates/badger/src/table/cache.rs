//! Block and index caches, weighted by bytes. Go uses ristretto (TinyLFU);
//! this uses `quick_cache` (S3-FIFO), which plays the same role.

use std::sync::Arc;

use bytes::Bytes;
use quick_cache::Weighter;
use quick_cache::sync::Cache;

use super::Block;

#[derive(Debug, Clone, Copy, Default)]
pub struct BlockWeighter;

impl Weighter<u64, Arc<Block>> for BlockWeighter {
    fn weight(&self, _key: &u64, block: &Arc<Block>) -> u64 {
        block.size() as u64
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct IndexWeighter;

impl Weighter<u64, Bytes> for IndexWeighter {
    fn weight(&self, _key: &u64, index: &Bytes) -> u64 {
        index.len() as u64
    }
}

/// Decoded blocks keyed by `(table id << 32) | block index`.
pub type BlockCache = Arc<Cache<u64, Arc<Block>, BlockWeighter>>;

/// Decrypted table indexes keyed by table id.
pub type IndexCache = Arc<Cache<u64, Bytes, IndexWeighter>>;

/// A block cache holding up to about `max_bytes` of blocks.
pub fn new_block_cache(max_bytes: u64) -> BlockCache {
    let items = (max_bytes / 4096).max(16) as usize;
    Arc::new(Cache::with_weighter(items, max_bytes, BlockWeighter))
}

/// An index cache holding up to about `max_bytes` of indexes.
pub fn new_index_cache(max_bytes: u64) -> IndexCache {
    let items = (max_bytes / (64 << 10)).max(16) as usize;
    Arc::new(Cache::with_weighter(items, max_bytes, IndexWeighter))
}
