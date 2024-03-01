use anyhow::{Result,anyhow};
use tempfile::tempfile;
use std::sync::Mutex;
use std::{collections::HashMap, sync::Arc};
use std::io::prelude::*;
use std::fs::File;
use std::io::SeekFrom;

#[derive(Clone, Default)]
pub struct EntityFileCache {
    id2pos: HashMap<String,(u64,u64)>,
    file_handle: Option<Arc<Mutex<File>>>,
    last_action_was_read: Arc<Mutex<bool>>,
}

impl EntityFileCache {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_entity(&mut self, entity: &str, json: &str) -> Result<()> {
        let fh = self.get_or_create_file_handle();
        let mut fh = fh.lock().map_err(|e|anyhow!(format!("{e}")))?;
        let before = fh.metadata()?.len();
        // Writes occur only at the end of the file, so only seek if the last action was "read"
        if *self.last_action_was_read.lock().map_err(|e|anyhow!(format!("{e}")))? {
            fh.seek(SeekFrom::End(0))?;
        }
        *self.last_action_was_read.lock().map_err(|e|anyhow!(format!("{e}")))? = false;
        fh.write_all(json.as_bytes())?;
        let after = fh.metadata()?.len();
        let diff = after-before;
        // println!("{entity}: {before} - {after} = {diff}");
        self.id2pos.insert(entity.to_string(),(before,diff));
        Ok(())
    }

    pub fn has_entity(&self, entity_id: &str) -> bool {
        self.id2pos.contains_key(entity_id)
    }

    pub fn get_entity(&self, entity_id: &str) -> Option<String> {
        let mut fh = match &self.file_handle {
            Some(fh) => fh.lock().ok()?,
            None => return None,
        };
        *self.last_action_was_read.lock().ok()? = true;
        let (start,length) = self.id2pos.get(entity_id)?;
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

    #[test]
    fn test_entity_file_cache() {
        let mut efc = EntityFileCache::new();
        efc.add_entity("Q123", "Foo").unwrap();
        efc.add_entity("Q456", "Bar").unwrap();
        efc.add_entity("Q789", "Baz").unwrap();
        let s = efc.get_entity("Q456");
        assert_eq!(s,Some("Bar".to_string()));
        let s = efc.get_entity("Nope");
        assert_eq!(s,None);
    }
}