//! Minimal async B-tree experiment over [`AsyncPageStore`].
//!
//! It intentionally supports create/open, put/get/forward range and checkpoint.
//! Delete/rebalance and overflow values remain on the synchronous production path.

#![allow(clippy::collapsible_if, clippy::never_loop)] // incremental experimental port

use rustc_hash::{FxHashMap, FxHashSet};

use crate::BTreeError;
use crate::async_page_store::{AsyncPageStore, PageStoreCommit, PageStoreMetadata, StoredPage};
use crate::page::{Page, ValueCell, decode_page, encode_page, page_fits};
use crate::superblock::Superblock;

const FIRST_DATA_PAGE: u64 = 2;

#[derive(Debug, Clone, Copy)]
pub struct AsyncBTreeOptions {
    pub page_size: usize,
    pub cache_pages: usize,
}
impl Default for AsyncBTreeOptions {
    fn default() -> Self {
        Self {
            page_size: 16 * 1024,
            cache_pages: 256,
        }
    }
}

pub struct AsyncOpfsBTree<S: AsyncPageStore> {
    store: S,
    options: AsyncBTreeOptions,
    root: Option<u64>,
    total_pages: u64,
    generation: u64,
    pages: FxHashMap<u64, Vec<u8>>,
    dirty: FxHashSet<u64>,
    access: FxHashMap<u64, u64>,
    tick: u64,
}
struct Split {
    key: Vec<u8>,
    right: u64,
}

impl<S: AsyncPageStore> AsyncOpfsBTree<S> {
    pub async fn open(store: S, options: AsyncBTreeOptions) -> Result<Self, BTreeError> {
        let mut tree = Self {
            store,
            options,
            root: None,
            total_pages: FIRST_DATA_PAGE,
            generation: 0,
            pages: FxHashMap::default(),
            dirty: FxHashSet::default(),
            access: FxHashMap::default(),
            tick: 0,
        };
        if tree.store.metadata().await?.is_some() {
            let slots = tree.store.read_pages(&[0, 1]).await?;
            let mut best = None;
            for page in slots {
                if let Ok(sb) = Superblock::decode_from_page(&page.bytes, tree.options.page_size) {
                    if best.is_none_or(|x: Superblock| sb.generation > x.generation) {
                        best = Some(sb);
                    }
                }
            }
            let sb = best.ok_or_else(|| BTreeError::Corrupt("no valid async superblock".into()))?;
            tree.root = (sb.root_page_id != 0).then_some(sb.root_page_id);
            tree.total_pages = sb.total_pages.max(FIRST_DATA_PAGE);
            tree.generation = sb.generation;
        } else {
            tree.checkpoint().await?;
        }
        Ok(tree)
    }
    pub fn into_store(self) -> S {
        self.store
    }
    async fn page(&mut self, id: u64) -> Result<Vec<u8>, BTreeError> {
        if let Some(p) = self.pages.get(&id).cloned() {
            self.touch(id);
            return Ok(p);
        }
        let mut found = self.store.read_pages(&[id]).await?;
        let p = found
            .pop()
            .ok_or_else(|| BTreeError::Io("page store returned no page".into()))?
            .bytes;
        self.pages.insert(id, p.clone());
        self.touch(id);
        self.evict();
        Ok(p)
    }
    fn touch(&mut self, id: u64) {
        self.tick += 1;
        self.access.insert(id, self.tick);
    }
    fn evict(&mut self) {
        while self.pages.len() > self.options.cache_pages {
            let victim = self
                .pages
                .keys()
                .filter(|id| !self.dirty.contains(id))
                .min_by_key(|id| self.access.get(id))
                .copied();
            let Some(id) = victim else { break };
            self.pages.remove(&id);
            self.access.remove(&id);
        }
    }
    fn alloc(&mut self) -> u64 {
        let id = self.total_pages;
        self.total_pages += 1;
        id
    }
    fn write(&mut self, id: u64, page: Page) -> Result<(), BTreeError> {
        let raw = encode_page(&page, self.options.page_size)?;
        self.pages.insert(id, raw);
        self.dirty.insert(id);
        self.touch(id);
        Ok(())
    }
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, BTreeError> {
        let mut id = match self.root {
            Some(id) => id,
            None => return Ok(None),
        };
        loop {
            match decode_page(&self.page(id).await?, self.options.page_size)? {
                Page::Leaf { entries, .. } => {
                    return Ok(entries
                        .into_iter()
                        .find(|(k, _)| k.as_slice() == key)
                        .and_then(|(_, v)| match v {
                            ValueCell::Inline(v) => Some(v),
                            ValueCell::Overflow { .. } => None,
                        }));
                }
                Page::Internal { keys, children } => {
                    id = children[keys.partition_point(|k| k.as_slice() <= key)];
                }
                _ => return Err(BTreeError::Corrupt("non-tree page in descent".into())),
            }
        }
    }
    pub async fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), BTreeError> {
        if value.len() > self.options.page_size / 2 {
            return Err(BTreeError::InvalidOptions(
                "async experiment does not yet support overflow values".into(),
            ));
        }
        if self.root.is_none() {
            let id = self.alloc();
            self.write(
                id,
                Page::Leaf {
                    entries: vec![(key.to_vec(), ValueCell::Inline(value.to_vec()))],
                    next: None,
                },
            )?;
            self.root = Some(id);
            return Ok(());
        }
        let root = self.root.expect("checked");
        if let Some(split) = self.insert(root, key, value).await? {
            let new_root = self.alloc();
            self.write(
                new_root,
                Page::Internal {
                    keys: vec![split.key],
                    children: vec![root, split.right],
                },
            )?;
            self.root = Some(new_root);
        }
        Ok(())
    }
    async fn insert(
        &mut self,
        id: u64,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Split>, BTreeError> {
        let page = decode_page(&self.page(id).await?, self.options.page_size)?;
        match page {
            Page::Leaf { mut entries, next } => {
                let at = entries.partition_point(|(k, _)| k.as_slice() < key);
                if entries.get(at).is_some_and(|(k, _)| k.as_slice() == key) {
                    entries[at].1 = ValueCell::Inline(value.to_vec());
                } else {
                    entries.insert(at, (key.to_vec(), ValueCell::Inline(value.to_vec())));
                }
                let candidate = Page::Leaf {
                    entries: entries.clone(),
                    next,
                };
                if page_fits(&candidate, self.options.page_size)? {
                    self.write(id, candidate)?;
                    return Ok(None);
                }
                let right_entries = entries.split_off(entries.len() / 2);
                let right = self.alloc();
                let separator = right_entries[0].0.clone();
                self.write(
                    right,
                    Page::Leaf {
                        entries: right_entries,
                        next,
                    },
                )?;
                self.write(
                    id,
                    Page::Leaf {
                        entries,
                        next: Some(right),
                    },
                )?;
                Ok(Some(Split {
                    key: separator,
                    right,
                }))
            }
            Page::Internal {
                mut keys,
                mut children,
            } => {
                let at = keys.partition_point(|k| k.as_slice() <= key);
                if let Some(split) = self.insert(children[at], key, value).await? {
                    keys.insert(at, split.key);
                    children.insert(at + 1, split.right);
                }
                let candidate = Page::Internal {
                    keys: keys.clone(),
                    children: children.clone(),
                };
                if page_fits(&candidate, self.options.page_size)? {
                    self.write(id, candidate)?;
                    return Ok(None);
                }
                let mid = keys.len() / 2;
                let separator = keys[mid].clone();
                let right_keys = keys.split_off(mid + 1);
                keys.pop();
                let right_children = children.split_off(mid + 1);
                let right = self.alloc();
                self.write(
                    right,
                    Page::Internal {
                        keys: right_keys,
                        children: right_children,
                    },
                )?;
                self.write(id, Page::Internal { keys, children })?;
                Ok(Some(Split {
                    key: separator,
                    right,
                }))
            }
            _ => Err(BTreeError::Corrupt("non-tree page in insert".into())),
        }
    }
    pub async fn range(
        &mut self,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BTreeError> {
        let mut out = Vec::new();
        if start >= end {
            return Ok(out);
        }
        let mut id = match self.root {
            Some(id) => id,
            None => return Ok(out),
        };
        loop {
            match decode_page(&self.page(id).await?, self.options.page_size)? {
                Page::Internal { keys, children } => {
                    id = children[keys.partition_point(|k| k.as_slice() <= start)]
                }
                Page::Leaf { entries, next } => loop {
                    for (k, v) in entries {
                        if k.as_slice() >= start && k.as_slice() < end {
                            if let ValueCell::Inline(v) = v {
                                out.push((k, v));
                                if out.len() == limit {
                                    return Ok(out);
                                }
                            }
                        }
                    }
                    let Some(n) = next else { return Ok(out) };
                    match decode_page(&self.page(n).await?, self.options.page_size)? {
                        Page::Leaf {
                            entries: e,
                            next: nx,
                        } => {
                            if e.first().is_some_and(|(k, _)| k.as_slice() >= end) {
                                return Ok(out);
                            };
                            id = n;
                            let _ = id;
                            return self.range_from_leaf(e, nx, start, end, limit, out).await;
                        }
                        _ => return Err(BTreeError::Corrupt("leaf link is not leaf".into())),
                    }
                },
                _ => return Err(BTreeError::Corrupt("non-tree page in range".into())),
            }
        }
    }
    async fn range_from_leaf(
        &mut self,
        mut entries: Vec<(Vec<u8>, ValueCell)>,
        mut next: Option<u64>,
        start: &[u8],
        end: &[u8],
        limit: usize,
        mut out: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, BTreeError> {
        loop {
            for (k, v) in entries {
                if k.as_slice() >= start && k.as_slice() < end {
                    if let ValueCell::Inline(v) = v {
                        out.push((k, v));
                        if out.len() == limit {
                            return Ok(out);
                        }
                    }
                }
            }
            let Some(id) = next else { return Ok(out) };
            match decode_page(&self.page(id).await?, self.options.page_size)? {
                Page::Leaf {
                    entries: e,
                    next: n,
                } => {
                    entries = e;
                    next = n
                }
                _ => return Err(BTreeError::Corrupt("leaf link is not leaf".into())),
            }
        }
    }
    pub async fn checkpoint(&mut self) -> Result<(), BTreeError> {
        self.generation += 1;
        let mut a = vec![0; self.options.page_size];
        let mut b = vec![0; self.options.page_size];
        let sb = Superblock::new(
            self.options.page_size as u32,
            self.generation,
            self.root.unwrap_or(0),
            0,
            self.total_pages,
        );
        sb.encode_into_page(&mut a)?;
        sb.encode_into_page(&mut b)?;
        let mut writes: Vec<_> = self
            .dirty
            .iter()
            .filter_map(|id| {
                self.pages.get(id).map(|bytes| StoredPage {
                    page_id: *id,
                    bytes: bytes.clone(),
                })
            })
            .collect();
        writes.push(StoredPage {
            page_id: 0,
            bytes: a,
        });
        writes.push(StoredPage {
            page_id: 1,
            bytes: b,
        });
        self.store
            .commit(PageStoreCommit {
                metadata: PageStoreMetadata {
                    page_size: self.options.page_size as u32,
                    logical_len: self.total_pages * self.options.page_size as u64,
                },
                writes,
                deleted_page_ids: vec![],
            })
            .await?;
        self.dirty.clear();
        self.evict();
        Ok(())
    }
}
