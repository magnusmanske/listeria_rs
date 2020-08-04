use crate::*;
use wikibase::entity::*;
use wikibase::snak::SnakDataType;
use wikibase::entity_container::EntityContainer;

#[derive(Debug, Clone)]
pub struct ListeriaList {
    page_params: PageParams,
    template: Template,
    pub columns: Vec<Column>,
    pub params: TemplateParams,
    sparql_rows: Vec<HashMap<String, SparqlValue>>,
    sparql_first_variable: Option<String>,
    entities: EntityContainer,
    pub results:Vec<ResultRow>,
    wikis_to_check_for_shadow_images: Vec<String>,
    shadow_files: Vec<String>,
    local_page_cache: HashMap<String,bool>,
}

impl ListeriaList {
    pub fn new(template:Template,page_params:PageParams) -> Self {
        Self {
            page_params:page_params,
            template: template,
            columns: vec![],
            params:TemplateParams::new(),
            sparql_rows: vec![],
            sparql_first_variable: None,
            entities: EntityContainer::new(),
            results: vec![],
            wikis_to_check_for_shadow_images: vec!["enwiki".to_string()],
            shadow_files: vec![],
            local_page_cache: HashMap::new(),
        }
    }

    pub fn shadow_files(&self) -> &Vec<String> {
        &self.shadow_files
    }

    pub fn local_file_namespace_prefix(&self) -> String {
        self.page_params.local_file_namespace_prefix()
    }

    pub fn process_template(&mut self) -> Result<(), String> {
        let template = self.template.clone();
        match template.params.get("columns") {
            Some(columns) => {
                columns.split(",").for_each(|part| {
                    let s = part.clone().to_string();
                    self.columns.push(Column::new(&s));
                });
            }
            None => self.columns.push(Column::new(&"item".to_string())),
        }

        self.params = TemplateParams {
            links:LinksType::All,
            sort: SortMode::new(template.params.get("sort")),
            section: template.params.get("section").map(|s|s.trim().to_uppercase()),
            min_section: template
                            .params
                            .get("min_section")
                            .map(|s|
                                s.parse::<u64>().ok().or(Some(2)).unwrap_or(2)
                                )
                            .unwrap_or(2),
            row_template: template.params.get("row_template").map(|s|s.trim().to_string()),
            header_template: template.params.get("header_template").map(|s|s.trim().to_string()),
            autolist: template.params.get("autolist").map(|s|s.trim().to_uppercase()) ,
            summary: template.params.get("summary").map(|s|s.trim().to_uppercase()) ,
            skip_table: template.params.get("skip_table").is_some(),
            one_row_per_item: template.params.get("one_row_per_item").map(|s|s.trim().to_uppercase())!=Some("NO".to_string()),
            wdedit: template.params.get("wdedit").map(|s|s.trim().to_uppercase())==Some("YES".to_string()),
            references: template.params.get("references").map(|s|s.trim().to_uppercase())==Some("ALL".to_string()),
            sort_ascending: template.params.get("sort_order").map(|s|s.trim().to_uppercase())!=Some("DESC".to_string()),
        } ;

        match template.params.get("language") {
            Some(l) =>  self.page_params.language = l.to_lowercase(),
            None => {}
        }

        match template.params.get("links") {
            Some(s) =>  self.params.links = LinksType::new_from_string(s.to_string()),
            None => {}
        }

        Ok(())
    }

    pub fn language(&self) -> &String {
        &self.page_params.language
    }


    async fn cache_local_page_exists(&mut self,page:String) {
        let params: HashMap<String, String> = vec![
            ("action", "query"),
            ("prop", ""),
            ("titles", page.as_str()),
        ]
        .iter()
        .map(|x| (x.0.to_string(), x.1.to_string()))
        .collect();

        let result = match self
            .page_params
            .mw_api
            .get_query_api_json(&params)
            .await {
                Ok(r) => r,
                Err(_e) => return
            };
            
        let page_exists = match result["query"]["pages"].as_object() {
            Some(obj) => {
                obj
                .iter()
                .filter(|(_k,v)|v["missing"].as_str().is_some())
                .count()==0 // No "missing"=existing
            }
            None => false // Dunno
        };
        self.local_page_cache.insert(page,page_exists);
    }

    pub fn local_page_exists(&self,page:&str) -> bool {
        *self.local_page_cache.get(&page.to_string()).unwrap_or(&false)
    }

    pub fn normalize_page_title(&self,s: &String) -> String {
        // TODO use page to find out about first character capitalization on the current wiki
        if s.len() < 2 {
            return s.to_string();
        }
        let (first_letter, the_rest) = s.split_at(1);
        return first_letter.to_uppercase() + the_rest;
    }

    pub fn get_location_template(&self, lat: f64, lon: f64) -> String {
        // Hardcoded special cases!!1!
        if self.page_params.wiki == "wikidatawiki" {
            return format!("{}/{}",lat,lon);
        }
        if self.page_params.wiki == "commonswiki" {
            return format!("{{Inline coordinates|{}|{}|display=inline}}}}",lat,lon);
        }
        if self.page_params.wiki == "dewiki" {
            // TODO get region for item
            let q = "" ;
            let region = "" ;
            return format!("{{{{Coordinate|text=DMS|NS={}|EW={}|name={}|simple=y|type=landmark|region={}}}}}",lat,lon,q,region);
        }
        format!("{{{{Coord|{}|{}|display=inline}}}}", lat, lon) // en; default
    }

    pub fn thumbnail_size(&self) -> u64 {
        let default: u64 = 128;
        match self.template.params.get("thumb") {
            Some(s) => s.parse::<u64>().ok().or(Some(default)).unwrap(),
            None => default,
        }
    }

    pub async fn run_query(&mut self) -> Result<(), String> {
        let sparql = match self.template.params.get("sparql") {
            Some(s) => s,
            None => return Err(format!("No `sparql` parameter in {:?}", &self.template)),
        };

        let j = match self.page_params.wd_api.sparql_query(sparql).await {
            Ok(j) => j,
            Err(e) => return Err(format!("{:?}", &e)),
        };
        self.parse_sparql(j)
    }

    fn parse_sparql(&mut self, j: Value) -> Result<(), String> {
        self.sparql_rows.clear();
        self.sparql_first_variable = None;
        
        // TODO force first_var to be "item" for backwards compatability?
        // Or check if it is, and fail if not?
        let first_var = match j["head"]["vars"].as_array() {
            Some(a) => match a.get(0) {
                Some(v) => v.as_str().ok_or("Can't parse first variable")?.to_string(),
                None => return Err(format!("Bad SPARQL head.vars")),
            },
            None => return Err(format!("Bad SPARQL head.vars")),
        };
        self.sparql_first_variable = Some(first_var.clone());

        let bindings = j["results"]["bindings"]
            .as_array()
            .ok_or("Broken SPARQL results.bindings")?;
        for b in bindings.iter() {
            let mut row: HashMap<String, SparqlValue> = HashMap::new();
            for (k, v) in b.as_object().unwrap().iter() {
                match SparqlValue::new_from_json(&v) {
                    Some(v2) => row.insert(k.to_owned(), v2),
                    None => return Err(format!("Can't parse SPARQL value: {} => {:?}", &k, &v)),
                };
            }
            if row.is_empty() {
                continue;
            }
            self.sparql_rows.push(row);
        }
        Ok(())
    }

    pub async fn load_entities(&mut self) -> Result<(), String> {
        // Any columns that require entities to be loaded?
        // TODO also force if self.links is redlinks etc.
        if self
            .columns
            .iter()
            .filter(|c| match c.obj {
                ColumnType::Number => false,
                ColumnType::Item => false,
                ColumnType::Field(_) => false,
                _ => true,
            })
            .count()
            == 0
        {
            return Ok(());
        }

        let ids = self.get_ids_from_sparql_rows()?;
        if ids.is_empty() {
            return Err(format!("No items to show"));
        }
        match self.entities.load_entities(&self.page_params.wd_api, &ids).await {
            Ok(_) => {}
            Err(e) => return Err(format!("Error loading entities: {:?}", &e)),
        }

        self.label_columns();

        Ok(())
    }

    fn label_columns(&mut self) {
        self.columns = self
            .columns
            .iter()
            .map(|c| {
                let mut c = c.clone();
                c.generate_label(self);
                c
            })
            .collect();
    }

    fn get_ids_from_sparql_rows(&self) -> Result<Vec<String>, String> {
        let varname = self.get_var_name()?;

        // Rows
        let ids_tmp: Vec<String> = self
            .sparql_rows
            .iter()
            .filter_map(|row| match row.get(varname) {
                Some(SparqlValue::Entity(id)) => Some(id.to_string()),
                _ => None,
            })
            .collect();

        let mut ids: Vec<String> = vec![] ;
        ids_tmp.iter().for_each(|id|{
            if !ids.contains(id) {
                ids.push(id.to_string());
            }
        });

        // Can't sort/dedup, need to preserve original order

        // Column headers
        self.columns.iter().for_each(|c| match &c.obj {
            ColumnType::Property(prop) => {
                ids.push(prop.to_owned());
            }
            ColumnType::PropertyQualifier((prop, qual)) => {
                ids.push(prop.to_owned());
                ids.push(qual.to_owned());
            }
            ColumnType::PropertyQualifierValue((prop1, qual, prop2)) => {
                ids.push(prop1.to_owned());
                ids.push(qual.to_owned());
                ids.push(prop2.to_owned());
            }
            _ => {}
        });

        Ok(ids)
    }

    fn get_var_name(&self) -> Result<&String, String> {
        match &self.sparql_first_variable {
            Some(v) => Ok(v),
            None => return Err(format!("load_entities: sparql_first_variable is None")),
        }
    }

    pub fn get_local_entity_label(&self, entity_id: &String) -> Option<String> {
        self.entities
            .get_entity(entity_id.to_owned())?
            .label_in_locale(&self.page_params.language)
            .map(|s| s.to_string())
    }

    fn entity_to_local_link(&self, item: &String) -> Option<ResultCellPart> {
        let entity = match self.entities.get_entity(item.to_owned()) {
            Some(e) => e,
            None => return None,
        };
        let page = match entity.sitelinks() {
            Some(sl) => sl
                .iter()
                .filter(|s| *s.site() == self.page_params.wiki)
                .map(|s| s.title().to_string())
                .next(),
            None => None,
        }?;
        let label = self.get_local_entity_label(item).unwrap_or(page.clone());
        Some(ResultCellPart::LocalLink((page, label)))
    }


    fn get_parts_p_p(&self,statement:&wikibase::statement::Statement,property:&String) -> Vec<ResultCellPart> {
        statement
            .qualifiers()
            .iter()
            .filter(|snak|*snak.property()==*property)
            .map(|snak|ResultCellPart::SnakList (
                    vec![
                        ResultCellPart::from_snak(statement.main_snak()),
                        ResultCellPart::from_snak(snak)
                    ]
                )
            )
            .collect()
    }

    fn get_parts_p_q_p(&self,statement:&wikibase::statement::Statement,target_item:&String,property:&String) -> Vec<ResultCellPart> {
        let links_to_target = match statement.main_snak().data_value(){
            Some(dv) => {
                match dv.value() {
                    wikibase::value::Value::Entity(e) => e.id() == target_item,
                    _ => false
                }
            }
            None => false
        };
        if !links_to_target {
            return vec![];
        }
        self.get_parts_p_p(statement,property)
    }

    fn get_result_cell(
        &self,
        entity_id: &String,
        sparql_rows: &Vec<&HashMap<String, SparqlValue>>,
        col: &Column,
    ) -> ResultCell {
        let mut ret = ResultCell::new();

        let entity = self.entities.get_entity(entity_id.to_owned());
        match &col.obj {
            ColumnType::Item => {
                ret.parts
                    .push(ResultCellPart::Entity((entity_id.to_owned(), true)));
            }
            ColumnType::Description => match entity {
                Some(e) => match e.description_in_locale(self.page_params.language.as_str()) {
                    Some(s) => {
                        ret.parts.push(ResultCellPart::Text(s.to_string()));
                    }
                    None => {}
                },
                None => {}
            },
            ColumnType::Field(varname) => {
                for row in sparql_rows.iter() {
                    match row.get(varname) {
                        Some(x) => {
                            ret.parts.push(ResultCellPart::from_sparql_value(x));
                        }
                        None => {}
                    }
                }
            }
            ColumnType::Property(property) => match entity {
                Some(e) => {
                    e.claims_with_property(property.to_owned())
                        .iter()
                        .for_each(|statement| {
                            ret.parts
                                .push(ResultCellPart::from_snak(statement.main_snak()));
                        });
                }
                None => {}
            },
            ColumnType::PropertyQualifier((p1, p2)) => match entity {
                Some(e) => {
                    e.claims_with_property(p1.to_owned())
                        .iter()
                        .for_each(|statement| {
                            self.get_parts_p_p(statement,p2)
                                .iter()
                                .for_each(|part|ret.parts.push(part.to_owned()));
                        });
                }
                None => {}
            },
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => match entity {
                Some(e) => {
                    e.claims_with_property(p1.to_owned())
                        .iter()
                        .for_each(|statement| {
                            self.get_parts_p_q_p(statement,q1,p2)
                                .iter()
                                .for_each(|part|ret.parts.push(part.to_owned()));
                        });
                }
                None => {}
            },
            ColumnType::LabelLang(language) => match entity {
                Some(e) => {
                    match e.label_in_locale(language) {
                        Some(s) => {
                            ret.parts.push(ResultCellPart::Text(s.to_string()));
                        }
                        None => match e.label_in_locale(&self.page_params.language) {
                            // Fallback
                            Some(s) => {
                                ret.parts.push(ResultCellPart::Text(s.to_string()));
                            }
                            None => {} // No label available
                        },
                    }
                }
                None => {}
            },
            ColumnType::Label => match entity {
                Some(e) => {
                    let label = match e.label_in_locale(&self.page_params.language) {
                        Some(s) => s.to_string(),
                        None => entity_id.to_string(),
                    };
                    let local_page = match e.sitelinks() {
                        Some(sl) => sl
                            .iter()
                            .filter(|s| *s.site() == self.page_params.wiki)
                            .map(|s| s.title().to_string())
                            .next(),
                        None => None,
                    };
                    match local_page {
                        Some(page) => {
                            ret.parts.push(ResultCellPart::LocalLink((page, label)));
                        }
                        None => {
                            ret.parts
                                .push(ResultCellPart::Entity((entity_id.to_string(), true)));
                        }
                    }
                }
                None => {}
            },
            ColumnType::Unknown => {} // Ignore
            ColumnType::Number => {
                ret.parts.push(ResultCellPart::Number);
            }
        }

        ret
    }

    fn get_result_row(
        &self,
        entity_id: &String,
        sparql_rows: &Vec<&HashMap<String, SparqlValue>>,
    ) -> Option<ResultRow> {
        match self.params.links {
            LinksType::Local => {
                if !self.entities.has_entity(entity_id.to_owned()) {
                    return None;
                }
            }
            _ => {}
        }

        let mut row = ResultRow::new(entity_id);
        row.cells = self
            .columns
            .iter()
            .map(|col| self.get_result_cell(entity_id, sparql_rows, col))
            .collect();
        Some(row)
    }

    pub fn generate_results(&mut self) -> Result<(), String> {
        let varname = self.get_var_name()?;
        self.results = match self.params.one_row_per_item {
            true => self
                .get_ids_from_sparql_rows()?
                .iter()
                .filter_map(|id| {
                    let sparql_rows: Vec<&HashMap<String, SparqlValue>> = self
                        .sparql_rows
                        .iter()
                        .filter(|row| match row.get(varname) {
                            Some(SparqlValue::Entity(v)) => v == id,
                            _ => false,
                        })
                        .collect();
                    if !sparql_rows.is_empty() {
                        self.get_result_row(id, &sparql_rows)
                    } else {
                        None
                    }
                })
                .collect(),
            false => self
                .sparql_rows
                .iter()
                .filter_map(|row| match row.get(varname) {
                    Some(SparqlValue::Entity(id)) => self.get_result_row(id, &vec![&row]),
                    _ => None,
                })
                .collect(),
        } ;
        Ok(())
    }

    fn localize_item_links_in_parts(&self,parts:&Vec<ResultCellPart>) -> Vec<ResultCellPart> {
        parts.iter()
        .map(|part| match part {
            ResultCellPart::Entity((item, true)) => {
                match self.entity_to_local_link(&item) {
                    Some(ll) => ll,
                    None => part.to_owned(),
                }
            }
            ResultCellPart::SnakList(v) => {
                ResultCellPart::SnakList(self.localize_item_links_in_parts(v))
            }
            _ => part.to_owned(),
        })
        .collect()
    }


    fn patch_items_to_local_links(&mut self) -> Result<(), String> {
        // Try to change items to local link
        // TODO mutate in place; fn in ResultRow. This is pathetic.
        self.results = self
            .results
            .iter()
            .map(|row| ResultRow {
                entity_id: row.entity_id.to_owned(),
                cells: row
                    .cells
                    .iter()
                    .map(|cell| ResultCell {
                        parts: self.localize_item_links_in_parts(&cell.parts),
                    })
                    .collect(),
                section:row.section,
                sortkey: row.sortkey.to_owned()
            })
            .collect();
        Ok(())
    }


    async fn patch_remove_shadow_files(&mut self) -> Result<(), String> {
        if !self.wikis_to_check_for_shadow_images.contains(&self.page_params.wiki) {
            return Ok(())
        }
        let mut files_to_check = vec![] ;
        for row in self.results.iter() {
            for cell in &row.cells {
                for part in &cell.parts {
                    match part {
                        ResultCellPart::File(file) => {
                            files_to_check.push(file);
                        }
                        _ => {}
                    }
                }
            }
        }
        files_to_check.sort_unstable();
        files_to_check.dedup();

        self.shadow_files.clear();

        // TODO better async
        for filename in files_to_check {
            let prefixed_filename = format!("{}:{}",self.page_params.local_file_namespace_prefix(),&filename) ;
            let params: HashMap<String, String> =
                vec![("action", "query"), ("titles", prefixed_filename.as_str()),("prop","imageinfo")]
                    .iter()
                    .map(|x| (x.0.to_string(), x.1.to_string()))
                    .collect();

            let j = match self.page_params.mw_api.get_query_api_json(&params).await {
                Ok(j) => j,
                Err(_e) => json!({})
            };

            let mut could_be_local = false ;
            match j["query"]["pages"].as_object() {
                Some(results) => {
                    results.iter().for_each(|(_k, o)|{
                        match o["imagerepository"].as_str() {
                            Some("shared") => {},
                            _ => { could_be_local = true ; }
                        }
                    })
                }
                None => { could_be_local = true ; }
            };

            if could_be_local {
                self.shadow_files.push(filename.to_string());
            }
        }

        self.shadow_files.sort();

        // Remove shadow files from data table
        // TODO this is less than ideal in terms of pretty code...
        let shadow_files = &self.shadow_files;
        self.results.iter_mut().for_each(|row|{
            row.cells.iter_mut().for_each(|cell|{
                cell.parts = cell.parts.iter().filter(|part|{
                    match part {
                        ResultCellPart::File(file) => !shadow_files.contains(file),
                        _ => true
                    }
                })
                .cloned()
                .collect();
            });
        });

        Ok(())
    }

    fn patch_redlinks_only(&mut self) -> Result<(), String> {
        if *self.get_links_type() != LinksType::RedOnly {
            return Ok(())
        }

        // Remove all rows with existing local page  
        // TODO better iter things
        self.results = self.results
            .iter()
            .filter(|row|{
                let entity = self.entities.get_entity(row.entity_id.to_owned()).unwrap();
                match entity.sitelinks() {
                    Some(sl) => {
                        sl
                        .iter()
                        .filter(|s| *s.site() == self.page_params.wiki)
                        .count() == 0
                    }
                    None => true, // No sitelinks, keep
                }
            })
            .cloned()
            .collect();
        Ok(())
    }

    async fn patch_redlinks(&mut self) -> Result<(), String> {
        if *self.get_links_type() != LinksType::RedOnly && *self.get_links_type() != LinksType::Red {
            return Ok(())
        }

        // Cache if local pages exist
        let mut ids = vec![] ;
        self.results.iter().for_each(|row|{
            row.cells.iter().for_each(|cell|{
                cell.parts
                    .iter()
                    .for_each(|part|{
                    match part {
                        ResultCellPart::Entity((id, _try_localize)) => {
                            ids.push(id);
                        }
                        _ => {}
                    }
                })
            });
        });

        ids.sort();
        ids.dedup();
        let mut labels = vec![] ;
        for id in ids {
            match self.get_entity(id.to_owned()) {
                Some(e) => match e.label_in_locale(self.language()) {
                    Some(l) => {
                        labels.push(l.to_string());
                    }
                    None => {}
                }
                None => {}
            }
        }

        labels.sort();
        labels.dedup();
        for label in labels {
            self.cache_local_page_exists(label).await;
        }

        Ok(())
    }

    fn get_datatype_for_property(&self,prop:&String) -> SnakDataType {
        match self.get_entity(prop) {
            Some(entity) => {
                match entity {
                    Entity::Property(p) => {
                        match p.datatype() {
                            Some(t) => t.to_owned(),
                            None => SnakDataType::String
                        }
                    }
                    _ => SnakDataType::String
                }
            }
            None => SnakDataType::String
        }
    }

    fn patch_sort_results(&mut self) -> Result<(), String> {
        let sortkeys : Vec<String> ;
        let mut datatype = SnakDataType::String ; // Default
        match &self.params.sort {
            SortMode::Label => {
                sortkeys = self.results
                    .iter()
                    .map(|row|row.get_sortkey_label(&self))
                    .collect();
            }
            SortMode::FamilyName => {
                sortkeys = self.results
                    .iter()
                    .map(|row|row.get_sortkey_family_name(&self))
                    .collect();
            }
            SortMode::Property(prop) => {
                datatype = self.get_datatype_for_property(prop);
                sortkeys = self.results
                    .iter()
                    .map(|row|row.get_sortkey_prop(&prop,&self,&datatype))
                    .collect();
            }
            _ => return Ok(())
        }

        // Apply sortkeys
        if self.results.len() != sortkeys.len() { // Paranoia
            return Err(format!("patch_sort_results: sortkeys length mismatch"));
        }
        self.results
            .iter_mut()
            .enumerate()
            .for_each(|(rownum, row)|row.set_sortkey(sortkeys[rownum].to_owned())) ;

        self.results.sort_by(|a, b| a.compare_to(b,&datatype));
        if !self.params.sort_ascending {
            self.results.reverse()
        }

        //self.results.iter().for_each(|row|println!("{}: {}",&row.entity_id,&row.sortkey));
        Ok(())
    }
    
    pub async fn patch_results(&mut self) -> Result<(), String> {
        self.gather_and_load_items().await? ;
        self.patch_redlinks_only()?;
        self.patch_items_to_local_links()?;
        self.patch_redlinks().await?;
        self.patch_remove_shadow_files().await?;
        self.patch_sort_results()?;
        Ok(())
    }

    pub fn get_links_type(&self) -> &LinksType {
        &self.params.links // TODO duplicate code
    }

    pub fn get_entity<S: Into<String>>(&self, entity_id: S) -> Option<wikibase::Entity> {
        self.entities.get_entity(entity_id)
    }

    pub fn external_id_url(&self, prop: &String, id: &String) -> Option<String> {
        let pi = self.entities.get_entity(prop.to_owned())?;
        pi.claims_with_property("P1630")
            .iter()
            .filter_map(|s| {
                let data_value = s.main_snak().data_value().to_owned()?;
                match data_value.value() {
                    wikibase::Value::StringValue(s) => 
                        Some(
                        s.to_owned()
                            .replace("$1", &urlencoding::decode(&id).ok()?.to_string()),
                    ),
                    _ => None,
                }
            })
            .next()
    }

    pub fn get_row_template(&self) -> &Option<String> {
        &self.params.row_template
    }


    async fn load_items(&mut self, mut entities_to_load:Vec<String>) -> Result<(), String> {
        entities_to_load.sort() ;
        entities_to_load.dedup();
        match self.entities.load_entities(&self.page_params.wd_api, &entities_to_load).await {
            Ok(_) => {}
            Err(e) => return Err(format!("Error loading entities: {:?}", &e)),
        }
        Ok(())
    }

    async fn gather_and_load_items_sort(&mut self) -> Result<(), String> {
        match &self.params.sort {
            SortMode::Property(prop) => {
                let mut entities_to_load = vec![];
                for row in self.results.iter() {
                    match self.entities.get_entity(row.entity_id.to_owned()) {
                        Some(entity) => {
                            entity
                                .claims()
                                .iter()
                                .filter(|statement|statement.property()==prop)
                                .map(|statement|statement.main_snak())
                                .filter(|snak|*snak.datatype()==SnakDataType::WikibaseItem)
                                .filter_map(|snak|snak.data_value().to_owned())
                                .map(|datavalue|datavalue.value().to_owned())
                                .filter_map(|value|match value {
                                    wikibase::value::Value::Entity(v) => Some(v.id().to_owned()),
                                    _ => None
                                })
                                .for_each(|id|entities_to_load.push(id.to_string()));
                        }
                        None => {}
                    }
                }
                self.load_items(entities_to_load).await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn gather_and_load_items(&mut self) -> Result<(), String> {
        // Gather items to load
        let mut entities_to_load : Vec<String> = vec![];
        for row in self.results.iter() {
            for cell in &row.cells {
                self.gather_entities_and_external_properties(&cell.parts)
                    .iter()
                    .for_each(|entity_id|entities_to_load.push(entity_id.to_string()));
            }
        }
        match &self.params.sort {
            SortMode::Property(prop) => {
                entities_to_load.push(prop.to_string());
            }
            _ => {}
        }
        self.load_items(entities_to_load).await?;
        self.gather_and_load_items_sort().await?;
        Ok(())
    }


    fn gather_entities_and_external_properties(&self,parts:&Vec<ResultCellPart>) -> Vec<String> {
        let mut entities_to_load = vec![];
        for part in parts {
            match part {
                ResultCellPart::Entity((item, true)) => {
                    entities_to_load.push(item.to_owned());
                }
                ResultCellPart::ExternalId((property, _id)) => {
                    entities_to_load.push(property.to_owned());
                }
                ResultCellPart::SnakList(v) => {
                    self.gather_entities_and_external_properties(&v)
                        .iter()
                        .for_each(|entity_id|entities_to_load.push(entity_id.to_string()))
                }
                _ => {}
            }
        }
        entities_to_load
    }

    pub fn column(&self,column_id:usize) -> Option<&Column> {
        self.columns.get(column_id)
    }

    pub fn skip_table(&self) -> bool {
        self.params.skip_table
    }

    pub fn get_section_ids(&self) -> Vec<usize> {
        let mut ret : Vec<usize> = self
            .results
            .iter()
            .map(|row|{row.section})
            .collect();
        ret.sort_unstable();
        ret.dedup();
        ret
    }

}