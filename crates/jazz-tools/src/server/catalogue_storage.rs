use crate::catalogue::CatalogueEntry;
use crate::storage::{Storage, StorageError};

pub(crate) type DynCatalogueStorage = Box<dyn CatalogueStorage + Send>;

pub(crate) trait CatalogueStorage {
    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError>;
    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError>;
    fn flush(&self) -> Result<(), StorageError>;
    fn flush_wal(&self) -> Result<(), StorageError>;
    fn close(&self) -> Result<(), StorageError>;
}

impl<T: Storage> CatalogueStorage for T {
    fn scan_catalogue_entries(&self) -> Result<Vec<CatalogueEntry>, StorageError> {
        Storage::scan_catalogue_entries(self)
    }

    fn upsert_catalogue_entry(&mut self, entry: &CatalogueEntry) -> Result<(), StorageError> {
        Storage::upsert_catalogue_entry(self, entry)
    }

    fn flush(&self) -> Result<(), StorageError> {
        Storage::flush(self)
    }

    fn flush_wal(&self) -> Result<(), StorageError> {
        Storage::flush_wal(self)
    }

    fn close(&self) -> Result<(), StorageError> {
        Storage::close(self)
    }
}
