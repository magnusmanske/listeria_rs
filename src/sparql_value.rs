use regex::Regex;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct LatLon {
    pub lat: f64,
    pub lon: f64,
}

impl LatLon {
    pub fn new(lat: f64, lon: f64) -> Self {
        Self { lat, lon }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SparqlValue {
    Entity(String),
    File(String),
    Uri(String),
    Time(String),
    Location(LatLon),
    Literal(String),
}

impl SparqlValue {
    pub fn new_from_json(j: &Value) -> Option<Self> {
        lazy_static! {
            static ref RE_ENTITY: Regex = Regex::new(r#"^https{0,1}://[^/]+/entity/([A-Z]\d+)$"#)
                .expect("RE_ENTITY does not parse");
            static ref RE_FILE: Regex =
                Regex::new(r#"^https{0,1}://[^/]+/wiki/Special:FilePath/(.+?)$"#)
                    .expect("RE_FILE does not parse");
            static ref RE_POINT: Regex =
                Regex::new(r#"^Point\((-{0,1}\d+[\.0-9]+) (-{0,1}\d+[\.0-9]+)\)$"#)
                    .expect("RE_POINT does not parse");
            static ref RE_DATE: Regex = Regex::new(r#"^([+-]{0,1}\d+-\d{2}-\d{2})T00:00:00Z$"#)
                .expect("RE_DATE does not parse");
        }
        let value = match j["value"].as_str() {
            Some(v) => v,
            None => return None,
        };
        match j["type"].as_str() {
            Some("uri") => match RE_ENTITY.captures(value) {
                Some(caps) => caps
                    .get(1)
                    .map(|caps1| SparqlValue::Entity(caps1.as_str().to_string())),
                None => match RE_FILE.captures(value) {
                    Some(caps) => match caps.get(1) {
                        Some(caps1) => {
                            let file = caps1.as_str().to_string();
                            let file = urlencoding::decode(&file).ok()?;
                            let file = file.replace('_', " ");
                            Some(SparqlValue::File(file))
                        }
                        None => None,
                    },
                    None => Some(SparqlValue::Uri(value.to_string())),
                },
            },
            Some("literal") => match j["datatype"].as_str() {
                Some("http://www.opengis.net/ont/geosparql#wktLiteral") => {
                    match RE_POINT.captures(value) {
                        Some(caps) => {
                            let lat: f64 = caps.get(2)?.as_str().parse().ok()?;
                            let lon: f64 = caps.get(1)?.as_str().parse().ok()?;
                            Some(SparqlValue::Location(LatLon::new(lat, lon)))
                        }
                        None => None,
                    }
                }
                Some("http://www.w3.org/2001/XMLSchema#dateTime") => {
                    let time = value.to_string();
                    let time = match RE_DATE.captures(value) {
                        Some(caps) => {
                            let date: String = caps.get(1)?.as_str().to_string();
                            date
                        }
                        None => time,
                    };
                    Some(SparqlValue::Time(time))
                }
                _ => Some(SparqlValue::Literal(value.to_string())),
            },
            Some("bnode") => j["value"].as_str().map(|value| SparqlValue::Literal(value.to_string())),
            _ => None,
        }
    }
}
