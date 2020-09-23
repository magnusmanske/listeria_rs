pub use crate::listeria_page::ListeriaPage;
pub use crate::listeria_list::ListeriaList;
pub use crate::render_wikitext::RendererWikitext;
pub use crate::render_tabbed_data::RendererTabbedData;
pub use crate::result_row::ResultRow;
pub use crate::result_cell_part::ResultCellPart;
pub use crate::column::*;
pub use crate::{SparqlValue,LinksType};
use serde_json::Value;
use wikibase::entity::EntityTrait;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct Reference {
    pub url: Option<String>,
    pub title: Option<String>,
    pub date: Option<String>,
    pub stated_in: Option<String>, // Item
    wikitext_cache: Option<String>,
}

impl Reference {
    pub fn new_from_snaks(snaks: &[wikibase::snak::Snak],language: &str) -> Option<Self> {
        let mut ret = Self { ..Default::default() } ;

        for snak in snaks.iter() {
            match snak.property() {
                "P854" => { // Reference URL
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::StringValue(url) = dv.value() {
                            ret.url = Some(url.to_owned()) ;
                        }
                    }
                }
                "P1476" => { // Title
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::MonoLingual(mlt) = dv.value() {
                            if mlt.language() == language {
                                ret.title = Some(mlt.text().to_owned()) ;
                            }
                        }
                    }
                }
                "P813" => { // Timestamp/last access
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::Time(tv) = dv.value() {
                            if let Some(pos) = tv.time().find('T') {
                                let (date,_) = tv.time().split_at(pos) ;
                                let mut date = date.replace('+',"").to_string();
                                if *tv.precision() >= 11 { // Day
                                    // Keep
                                } else if *tv.precision() == 10 { // Month
                                    if let Some(pos) = date.rfind('-') {
                                        date = date.split_at(pos).0.to_string();
                                    }
                                } else if *tv.precision() <=9 { // Year etc TODO century etc
                                    if let Some(pos) = date.find('-') {
                                        date = date.split_at(pos).0.to_string();
                                    }
                                }
                                ret.date = Some(date);
                            }
                        }
                    }
                }
                "P248" => { // Stated in
                    if let Some(dv) = snak.data_value() {
                        if let wikibase::Value::Entity(item) = dv.value() {
                            ret.stated_in = Some(item.id().to_owned()) ;
                        }
                    }
                }
                _ => {}
            }
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret)
        }
    }

    fn is_empty(&self) -> bool {
        self.url.is_none() && self.stated_in.is_none()
    }

    pub fn as_wikitext(&mut self) -> String {
        match &self.wikitext_cache {
            Some(s) => return s.to_string(),
            None => {}
        }
        let mut s = String::new() ;

        if self.title.is_some() && self.url.is_some() {
            s += &format!("{{{{cite web|url={}|title={}",self.url.as_ref().unwrap(),self.title.as_ref().unwrap());
            if let Some(stated_in) = &self.stated_in {
                s += &format!("|website={}",stated_in); // TODO render item title
            }
            if let Some(date) = &self.date {
                s += &format!("|access-date={}",&date);
            }
            s += "}}";
        } else if self.url.is_some() {
            s += &self.url.as_ref().unwrap();
        } else if self.stated_in.is_some() {
            s += &self.stated_in.as_ref().unwrap(); // TODO render item title
        }

        self.wikitext_cache = Some(s);
        self.as_wikitext()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct PartWithReference {
    pub part: ResultCellPart,
    pub references: Option<Vec<Reference>>,
}

impl PartWithReference {
    pub fn new(part:ResultCellPart,references:Option<Vec<Reference>>) -> Self {
        Self {part,references}
    }

    pub fn as_wikitext(
        &self,
        list: &ListeriaList,
        rownum: usize,
        colnum: usize,
        partnum: usize,
    ) -> String {
        self.part.as_wikitext(list,rownum,colnum,partnum)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ResultCell {
    parts: Vec<PartWithReference>,
    wdedit_class: Option<String>,
}

impl ResultCell {
    pub async fn new(
        list:&ListeriaList,
        entity_id: &str,
        sparql_rows: &[&HashMap<String, SparqlValue>],
        col: &Column,
    ) -> Self {
        let mut ret = Self { parts:vec![] , wdedit_class:None };

        let entity = list.get_entity(entity_id.to_owned());
        match &col.obj {
            ColumnType::Item => {
                ret.parts.push(PartWithReference::new(ResultCellPart::Entity((entity_id.to_owned(), false)),None));
            }
            ColumnType::Description => if let Some(e) = entity { match e.description_in_locale(list.language()) {
                Some(s) => {
                    ret.wdedit_class = Some("wd_desc".to_string());
                    ret.parts.push(PartWithReference::new(ResultCellPart::Text(s.to_string()),None));
                }
                None => {
                    if let Ok(s) = list.get_autodesc_description(&e).await {
                        ret.parts.push(PartWithReference::new(ResultCellPart::Text(s),None));
                    }
                }
            } },
            ColumnType::Field(varname) => {
                for row in sparql_rows.iter() {
                    if let Some(x) = row.get(varname) {
                        ret.parts.push(PartWithReference::new(ResultCellPart::from_sparql_value(x),None));
                    }
                }
            }
            ColumnType::Property(property) => if let Some(e) = entity {
                ret.wdedit_class = Some(format!("wd_{}",property.to_lowercase()));
                list.get_filtered_claims(&e,property)
                    .iter()
                    .for_each(|statement| {
                        let references = Self::get_references_for_statement(&statement,list.language());
                        ret.parts
                            .push(PartWithReference::new(ResultCellPart::from_snak(statement.main_snak()),references));
                    });
            },
            ColumnType::PropertyQualifier((p1, p2)) => if let Some(e) = entity {
                list.get_filtered_claims(&e,p1)
                    .iter()
                    .for_each(|statement| {
                        ret.get_parts_p_p(statement,p2)
                            .iter()
                            .for_each(|part|ret.parts.push(PartWithReference::new(part.to_owned(),None)));
                    });
            },
            ColumnType::PropertyQualifierValue((p1, q1, p2)) => if let Some(e) = entity {
                list.get_filtered_claims(&e,p1)
                    .iter()
                    .for_each(|statement| {
                        ret.get_parts_p_q_p(statement,q1,p2)
                            .iter()
                            .for_each(|part|ret.parts.push(PartWithReference::new(part.to_owned(),None)));
                    });
            },
            ColumnType::LabelLang(language) => if let Some(e) = entity {
                match e.label_in_locale(language) {
                    Some(s) => {
                        ret.parts.push(PartWithReference::new(ResultCellPart::Text(s.to_string()),None));
                    }
                    None => if let Some(s) = e.label_in_locale(list.language()) {
                        ret.parts.push(PartWithReference::new(ResultCellPart::Text(s.to_string()),None));
                    },
                }
            },
            ColumnType::Label => if let Some(e) = entity {
                ret.wdedit_class = Some("wd_label".to_string());
                let label = match e.label_in_locale(list.language()) {
                    Some(s) => s.to_string(),
                    None => entity_id.to_string(),
                };
                let local_page = match e.sitelinks() {
                    Some(sl) => sl
                        .iter()
                        .filter(|s| *s.site() == *list.wiki())
                        .map(|s| s.title().to_string())
                        .next(),
                    None => None,
                };
                match local_page {
                    Some(page) => {
                        ret.parts.push(PartWithReference::new(ResultCellPart::LocalLink((page, label)),None));
                    }
                    None => {
                        ret.parts
                            .push(PartWithReference::new(ResultCellPart::Entity((entity_id.to_string(), true)),None));
                    }
                }
            },
            ColumnType::Unknown => {} // Ignore
            ColumnType::Number => {
                ret.parts.push(PartWithReference::new(ResultCellPart::Number,None));
            }
        }

        ret
    }

    fn get_parts_p_p(&self,statement:&wikibase::statement::Statement,property:&str) -> Vec<ResultCellPart> {
        statement
            .qualifiers()
            .iter()
            .filter(|snak|*snak.property()==*property)
            .map(|snak|ResultCellPart::SnakList (
                    vec![
                        PartWithReference::new(
                            ResultCellPart::from_snak(statement.main_snak()),
                            None
                        ),
                        PartWithReference::new(
                            ResultCellPart::from_snak(snak),
                            None
                        )
                    ]
                )
            )
            .collect()
    }

    fn get_references_for_statement(statement: &wikibase::statement::Statement,language:&str) -> Option<Vec<Reference>> {
        let references = statement.references() ;
        let mut ret : Vec<Reference> = vec![] ;
        for reference in references.iter() {
            if let Some(r) = Reference::new_from_snaks(reference.snaks(),language) {
                ret.push(r);    
            }

        }
        if ret.is_empty() { None } else { Some(ret) }
    }

    fn get_parts_p_q_p(&self,statement:&wikibase::statement::Statement,target_item:&str,property:&str) -> Vec<ResultCellPart> {
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

    pub fn get_sortkey(&self) -> String {
        match self.parts.get(0) {
            Some(part_with_reference) => {
                match &part_with_reference.part {
                    ResultCellPart::Entity((id,_)) => id.to_owned(),
                    ResultCellPart::LocalLink((page,_label)) => page.to_owned(),
                    ResultCellPart::Time(time) => time.to_owned(),
                    ResultCellPart::File(s) => s.to_owned(),
                    ResultCellPart::Uri(s) => s.to_owned(),
                    ResultCellPart::Text(s) => s.to_owned(),
                    ResultCellPart::ExternalId((_prop,id)) => id.to_owned(),
                    _ => String::new()
                }
            }
            None => String::new()
        }
    }

    pub fn parts(&self) -> &Vec<PartWithReference> {
        &self.parts
    }

    pub fn parts_mut(&mut self) -> &mut Vec<PartWithReference> {
        &mut self.parts
    }

    pub fn set_parts(&mut self, parts:Vec<PartWithReference> ) {
        self.parts = parts ;
    }

    pub fn localize_item_links_in_parts(list: &ListeriaList, parts: &mut Vec<PartWithReference>) {
        for part_with_reference in parts.iter_mut() {
            part_with_reference.part.localize_item_links(list);
        }
    }

    pub fn as_tabbed_data(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> Value {
        let ret: Vec<String> = self
            .parts
            .iter()
            .enumerate()
            .map(|(partnum, part_with_reference)| part_with_reference.part.as_tabbed_data(list, rownum, colnum, partnum))
            .collect();
        json!(ret.join("<br/>"))
    }

    pub fn as_wikitext(&self, list: &ListeriaList, rownum: usize, colnum: usize) -> String {
        let mut ret ;
        if list.template_params().wdedit {
            ret = match &self.wdedit_class {
                Some(class) => format!("class='{}'| ",class.to_owned()),
                None => " ".to_string()
            };
        } else {
            ret = " ".to_string();
        }
        ret += &self.parts
            .iter()
            .enumerate()
            .map(|(partnum, part_with_reference)| part_with_reference.as_wikitext(list, rownum, colnum, partnum))
            .collect::<Vec<String>>()
            .join("<br/>") ;
        ret
    }
}
