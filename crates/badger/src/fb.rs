//! FlatBuffers for the SSTable index, from Go Badger's `fb/flatbuffer.fbs`:
//!
//! ```text
//! table TableIndex {
//!   offsets:[BlockOffset];  bloom_filter:[ubyte];  max_version:uint64;
//!   key_count:uint32;  uncompressed_size:uint32;  on_disk_size:uint32;
//!   stale_data_size:uint32;
//! }
//! table BlockOffset { key:[ubyte]; offset:uint; len:uint; }
//! ```
//!
//! Written by hand in the shape `flatc --rust` generates (accessors, verifier,
//! builders), so building needs no `flatc`. Vtable slot numbers follow field
//! order: slot `n` is at voffset `4 + 2n`.

use flatbuffers::{
    FlatBufferBuilder, Follow, ForwardsUOffset, InvalidFlatbuffer, Table, VOffsetT, Vector,
    Verifiable, Verifier, WIPOffset,
};

// ---------------------------------------------------------------------------
// BlockOffset

/// Location and first key of one data block in an SSTable.
#[derive(Copy, Clone, PartialEq)]
pub struct BlockOffset<'a> {
    tab: Table<'a>,
}

impl<'a> Follow<'a> for BlockOffset<'a> {
    type Inner = BlockOffset<'a>;
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        // SAFETY: the caller guarantees `loc` points at a BlockOffset table in `buf`.
        let tab = unsafe { Table::new(buf, loc) };
        BlockOffset { tab }
    }
}

impl<'a> BlockOffset<'a> {
    pub const VT_KEY: VOffsetT = 4;
    pub const VT_OFFSET: VOffsetT = 6;
    pub const VT_LEN: VOffsetT = 8;

    /// The first key of the block (an internal key).
    pub fn key(&self) -> Option<Vector<'a, u8>> {
        // SAFETY: the buffer was verified, or built by us, so the field has this type.
        unsafe {
            self.tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_KEY, None)
        }
    }

    /// Offset of the block from the start of the table.
    pub fn offset(&self) -> u32 {
        // SAFETY: as above.
        unsafe { self.tab.get::<u32>(Self::VT_OFFSET, Some(0)).unwrap() }
    }

    /// Length of the block in bytes.
    pub fn len(&self) -> u32 {
        // SAFETY: as above.
        unsafe { self.tab.get::<u32>(Self::VT_LEN, Some(0)).unwrap() }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Verifiable for BlockOffset<'_> {
    fn run_verifier(v: &mut Verifier, pos: usize) -> Result<(), InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>("key", Self::VT_KEY, false)?
            .visit_field::<u32>("offset", Self::VT_OFFSET, false)?
            .visit_field::<u32>("len", Self::VT_LEN, false)?
            .finish();
        Ok(())
    }
}

impl std::fmt::Debug for BlockOffset<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlockOffset")
            .field("key", &self.key().map(|k| k.bytes()))
            .field("offset", &self.offset())
            .field("len", &self.len())
            .finish()
    }
}

/// Builds a BlockOffset table. Call the `add_*` methods in the same order as
/// Go's builder to get identical bytes.
pub struct BlockOffsetBuilder<'a: 'b, 'b> {
    fbb: &'b mut FlatBufferBuilder<'a>,
    start: WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}

impl<'a: 'b, 'b> BlockOffsetBuilder<'a, 'b> {
    pub fn new(fbb: &'b mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        BlockOffsetBuilder { fbb, start }
    }

    pub fn add_key(&mut self, key: WIPOffset<Vector<'b, u8>>) {
        self.fbb
            .push_slot_always::<WIPOffset<_>>(BlockOffset::VT_KEY, key);
    }

    pub fn add_offset(&mut self, offset: u32) {
        self.fbb.push_slot::<u32>(BlockOffset::VT_OFFSET, offset, 0);
    }

    pub fn add_len(&mut self, len: u32) {
        self.fbb.push_slot::<u32>(BlockOffset::VT_LEN, len, 0);
    }

    pub fn finish(self) -> WIPOffset<BlockOffset<'a>> {
        let o = self.fbb.end_table(self.start);
        WIPOffset::new(o.value())
    }
}

// ---------------------------------------------------------------------------
// TableIndex

/// The index at the end of every SSTable.
#[derive(Copy, Clone, PartialEq)]
pub struct TableIndex<'a> {
    tab: Table<'a>,
}

impl<'a> Follow<'a> for TableIndex<'a> {
    type Inner = TableIndex<'a>;
    unsafe fn follow(buf: &'a [u8], loc: usize) -> Self::Inner {
        // SAFETY: the caller guarantees `loc` points at a TableIndex table in `buf`.
        let tab = unsafe { Table::new(buf, loc) };
        TableIndex { tab }
    }
}

impl<'a> TableIndex<'a> {
    pub const VT_OFFSETS: VOffsetT = 4;
    pub const VT_BLOOM_FILTER: VOffsetT = 6;
    pub const VT_MAX_VERSION: VOffsetT = 8;
    pub const VT_KEY_COUNT: VOffsetT = 10;
    pub const VT_UNCOMPRESSED_SIZE: VOffsetT = 12;
    pub const VT_ON_DISK_SIZE: VOffsetT = 14;
    pub const VT_STALE_DATA_SIZE: VOffsetT = 16;

    pub fn offsets(&self) -> Option<Vector<'a, ForwardsUOffset<BlockOffset<'a>>>> {
        // SAFETY: the buffer was verified, or built by us, so the field has this type.
        unsafe {
            self.tab
                .get::<ForwardsUOffset<Vector<'a, ForwardsUOffset<BlockOffset<'a>>>>>(
                    Self::VT_OFFSETS,
                    None,
                )
        }
    }

    /// The bloom filter over all keys of the table (see [`crate::util::bloom`]).
    pub fn bloom_filter(&self) -> Option<Vector<'a, u8>> {
        // SAFETY: as above.
        unsafe {
            self.tab
                .get::<ForwardsUOffset<Vector<'a, u8>>>(Self::VT_BLOOM_FILTER, None)
        }
    }

    pub fn max_version(&self) -> u64 {
        // SAFETY: as above.
        unsafe { self.tab.get::<u64>(Self::VT_MAX_VERSION, Some(0)).unwrap() }
    }

    pub fn key_count(&self) -> u32 {
        // SAFETY: as above.
        unsafe { self.tab.get::<u32>(Self::VT_KEY_COUNT, Some(0)).unwrap() }
    }

    pub fn uncompressed_size(&self) -> u32 {
        // SAFETY: as above.
        unsafe {
            self.tab
                .get::<u32>(Self::VT_UNCOMPRESSED_SIZE, Some(0))
                .unwrap()
        }
    }

    pub fn on_disk_size(&self) -> u32 {
        // SAFETY: as above.
        unsafe { self.tab.get::<u32>(Self::VT_ON_DISK_SIZE, Some(0)).unwrap() }
    }

    pub fn stale_data_size(&self) -> u32 {
        // SAFETY: as above.
        unsafe {
            self.tab
                .get::<u32>(Self::VT_STALE_DATA_SIZE, Some(0))
                .unwrap()
        }
    }
}

impl Verifiable for TableIndex<'_> {
    fn run_verifier(v: &mut Verifier, pos: usize) -> Result<(), InvalidFlatbuffer> {
        v.visit_table(pos)?
            .visit_field::<ForwardsUOffset<Vector<'_, ForwardsUOffset<BlockOffset<'_>>>>>(
                "offsets",
                Self::VT_OFFSETS,
                false,
            )?
            .visit_field::<ForwardsUOffset<Vector<'_, u8>>>(
                "bloom_filter",
                Self::VT_BLOOM_FILTER,
                false,
            )?
            .visit_field::<u64>("max_version", Self::VT_MAX_VERSION, false)?
            .visit_field::<u32>("key_count", Self::VT_KEY_COUNT, false)?
            .visit_field::<u32>("uncompressed_size", Self::VT_UNCOMPRESSED_SIZE, false)?
            .visit_field::<u32>("on_disk_size", Self::VT_ON_DISK_SIZE, false)?
            .visit_field::<u32>("stale_data_size", Self::VT_STALE_DATA_SIZE, false)?
            .finish();
        Ok(())
    }
}

impl std::fmt::Debug for TableIndex<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableIndex")
            .field("offsets", &self.offsets().map(|o| o.len()))
            .field("bloom_filter_len", &self.bloom_filter().map(|b| b.len()))
            .field("max_version", &self.max_version())
            .field("key_count", &self.key_count())
            .field("uncompressed_size", &self.uncompressed_size())
            .field("on_disk_size", &self.on_disk_size())
            .field("stale_data_size", &self.stale_data_size())
            .finish()
    }
}

/// Builds a TableIndex table. Call the `add_*` methods in the same order as
/// Go's `table.Builder.buildIndex` to get identical bytes.
pub struct TableIndexBuilder<'a: 'b, 'b> {
    fbb: &'b mut FlatBufferBuilder<'a>,
    start: WIPOffset<flatbuffers::TableUnfinishedWIPOffset>,
}

impl<'a: 'b, 'b> TableIndexBuilder<'a, 'b> {
    pub fn new(fbb: &'b mut FlatBufferBuilder<'a>) -> Self {
        let start = fbb.start_table();
        TableIndexBuilder { fbb, start }
    }

    pub fn add_offsets(&mut self, v: WIPOffset<Vector<'b, ForwardsUOffset<BlockOffset<'b>>>>) {
        self.fbb
            .push_slot_always::<WIPOffset<_>>(TableIndex::VT_OFFSETS, v);
    }

    pub fn add_bloom_filter(&mut self, v: WIPOffset<Vector<'b, u8>>) {
        self.fbb
            .push_slot_always::<WIPOffset<_>>(TableIndex::VT_BLOOM_FILTER, v);
    }

    pub fn add_max_version(&mut self, v: u64) {
        self.fbb.push_slot::<u64>(TableIndex::VT_MAX_VERSION, v, 0);
    }

    pub fn add_key_count(&mut self, v: u32) {
        self.fbb.push_slot::<u32>(TableIndex::VT_KEY_COUNT, v, 0);
    }

    pub fn add_uncompressed_size(&mut self, v: u32) {
        self.fbb
            .push_slot::<u32>(TableIndex::VT_UNCOMPRESSED_SIZE, v, 0);
    }

    pub fn add_on_disk_size(&mut self, v: u32) {
        self.fbb.push_slot::<u32>(TableIndex::VT_ON_DISK_SIZE, v, 0);
    }

    pub fn add_stale_data_size(&mut self, v: u32) {
        self.fbb
            .push_slot::<u32>(TableIndex::VT_STALE_DATA_SIZE, v, 0);
    }

    pub fn finish(self) -> WIPOffset<TableIndex<'a>> {
        let o = self.fbb.end_table(self.start);
        WIPOffset::new(o.value())
    }
}

/// Verifies `buf` and returns its root TableIndex.
pub fn root_as_table_index(buf: &[u8]) -> Result<TableIndex<'_>, InvalidFlatbuffer> {
    flatbuffers::root::<TableIndex>(buf)
}

/// Returns the root TableIndex without verifying it.
///
/// # Safety
/// The same bytes must already have passed [`root_as_table_index`].
pub unsafe fn root_as_table_index_unchecked(buf: &[u8]) -> TableIndex<'_> {
    // SAFETY: the caller guarantees `buf` is a verified TableIndex buffer.
    unsafe { flatbuffers::root_unchecked::<TableIndex>(buf) }
}

/// Overwrites `on_disk_size` in a finished TableIndex buffer, like Go's
/// `MutateOnDiskSize`. Returns false if the field is absent (it was written
/// as the default 0), in which case nothing changes.
///
/// The buffer must already have passed [`root_as_table_index`].
pub fn mutate_on_disk_size(buf: &mut [u8], value: u32) -> bool {
    let Ok(index) = root_as_table_index(buf) else {
        return false;
    };
    let table_pos = index.tab.loc();
    let field = index.tab.vtable().get(TableIndex::VT_ON_DISK_SIZE) as usize;
    if field == 0 {
        return false;
    }
    let pos = table_pos + field;
    buf[pos..pos + 4].copy_from_slice(&value.to_le_bytes());
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_and_read_back() {
        let mut fbb = FlatBufferBuilder::new();
        let key = fbb.create_vector(b"first-key");
        let mut bo = BlockOffsetBuilder::new(&mut fbb);
        bo.add_key(key);
        bo.add_offset(0);
        bo.add_len(4096);
        let bo = bo.finish();
        let offsets = fbb.create_vector(&[bo]);
        let bloom = fbb.create_vector(&[1u8, 2, 3]);
        let mut ti = TableIndexBuilder::new(&mut fbb);
        ti.add_offsets(offsets);
        ti.add_bloom_filter(bloom);
        ti.add_max_version(7);
        ti.add_key_count(10);
        ti.add_on_disk_size(100);
        let root = ti.finish();
        fbb.finish(root, None);

        let mut buf = fbb.finished_data().to_vec();
        assert!(mutate_on_disk_size(&mut buf, 200));
        let index = root_as_table_index(&buf).unwrap();
        assert_eq!(index.max_version(), 7);
        assert_eq!(index.key_count(), 10);
        assert_eq!(index.on_disk_size(), 200);
        assert_eq!(index.stale_data_size(), 0);
        assert_eq!(index.bloom_filter().unwrap().bytes(), &[1, 2, 3]);
        let first = index.offsets().unwrap().get(0);
        assert_eq!(
            (first.key().unwrap().bytes(), first.offset(), first.len()),
            (&b"first-key"[..], 0, 4096)
        );
    }

    #[test]
    fn rejects_garbage() {
        assert!(root_as_table_index(&[0xff; 16]).is_err());
    }
}
