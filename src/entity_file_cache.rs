use anyhow::Result;
use tempfile::tempfile;
use tokio::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
// use std::io;
use std::io::prelude::*;
use std::fs::File;
use std::io::SeekFrom;

#[derive(Clone, Default)]
pub struct EntityFileCache {
    id2pos: HashMap<String,(u64,u64)>,
    file_handle: Option<Arc<Mutex<File>>>,
}

impl EntityFileCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn add_entity(&mut self, entity: &str, json: &str) -> Result<()> {
        let fh = self.get_or_create_file_handle();
        let mut fh = fh.lock().await;
        let before = fh.metadata()?.len();
        fh.seek(SeekFrom::End(0))?; // TODO only if required
        fh.write_all(json.as_bytes())?;
        let after = fh.metadata()?.len();
        let diff = after-before;
        // println!("{entity}: {before} - {after} = {diff}");
        self.id2pos.insert(entity.to_string(),(before,diff));
        Ok(())
    }

    pub async fn get_entity(&self, entity: &str) -> Option<String> {
        let mut fh = match &self.file_handle {
            Some(fh) => fh.lock().await,
            None => return None,
        };
        let (start,length) = self.id2pos.get(entity)?;
        fh.seek(SeekFrom::Start(*start)).ok()?;
        let mut buffer: Vec<u8> = Vec::new();
        buffer.resize(*length as usize, 0);
        fh.read_exact(&mut buffer).ok()?;
        let s: String = String::from_utf8(buffer).ok()?;
        Some(s)
    }

    fn get_or_create_file_handle(&mut self) -> Arc<Mutex<File>> {
        if let Some(fh) = &self.file_handle { return fh.clone(); }
        let fh = tempfile().expect("EntityFileCache::get_or_create_file_handle: Could not create temporary file");
        self.file_handle = Some(Arc::new(Mutex::new(fh))); // Should auto-destruct
        if let Some(fh) = &self.file_handle { return fh.clone(); }
        panic!("EntityFileCache::get_or_create_file_handle: This is weird");
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_entity_file_cache() {
        let mut efc = EntityFileCache::new();
        efc.add_entity("Q123", "Foo").await.unwrap();
        efc.add_entity("Q456", "Bar").await.unwrap();
        efc.add_entity("Q789", "Baz").await.unwrap();
        let s = efc.get_entity("Q456").await;
        assert_eq!(s,Some("Bar".to_string()));
        let s = efc.get_entity("Nope").await;
        assert_eq!(s,None);
    }
}