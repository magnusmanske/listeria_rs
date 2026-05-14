//! Conversion from Wikibase `Snak` / SPARQL `SparqlValue` values into
//! `ResultCellPart` variants, plus the helpers that parse Wikidata time
//! strings.
//!
//! Pure data → data transforms with no rendering logic; rendering lives in
//! the parent module's `as_wikitext_*` helpers.

use super::{EntityInfo, ExternalIdInfo, LocationInfo, ResultCellPart};
use era_date::{Era, Precision};
use wikimisc::sparql_value::SparqlValue;
use wikimisc::wikibase::{Snak, SnakDataType, TimeValue, Value};

impl ResultCellPart {
    pub fn from_sparql_value(v: &SparqlValue) -> Self {
        match v {
            SparqlValue::Entity(x) => ResultCellPart::Entity(EntityInfo::new(x.to_owned(), true)),
            SparqlValue::File(x) => ResultCellPart::File(x.to_owned()),
            SparqlValue::Uri(x) => ResultCellPart::Uri(x.to_owned()),
            SparqlValue::Time(x) => ResultCellPart::Text(x.to_owned()),
            SparqlValue::Location(x) => {
                ResultCellPart::Location(LocationInfo::new(x.lat, x.lon, None))
            }
            SparqlValue::Literal(x) => ResultCellPart::Text(x.to_owned()),
        }
    }

    pub fn from_snak(snak: &Snak) -> Self {
        let Some(dv) = &snak.data_value() else {
            return ResultCellPart::Text("No/unknown value".to_string());
        };
        match dv.value() {
            Value::Entity(v) => ResultCellPart::Entity(EntityInfo::new(v.id().to_string(), true)),
            Value::StringValue(v) => Self::from_snak_string(snak, v),
            Value::Quantity(v) => {
                ResultCellPart::Quantity(*v.amount(), Self::unit_entity_id_from_url(v.unit()))
            }
            Value::Time(v) => Self::from_snak_time(v),
            Value::Coordinate(v) => {
                ResultCellPart::Location(LocationInfo::new(*v.latitude(), *v.longitude(), None))
            }
            Value::MonoLingual(v) => {
                ResultCellPart::Text(v.language().to_string() + ":" + v.text())
            }
            Value::EntitySchema(v) => ResultCellPart::EntitySchema(v.id().to_string()),
        }
    }

    fn from_snak_string(snak: &Snak, v: &str) -> Self {
        match snak.datatype() {
            SnakDataType::CommonsMedia => ResultCellPart::File(v.to_string()),
            SnakDataType::ExternalId => ResultCellPart::ExternalId(ExternalIdInfo::new(
                snak.property().to_string(),
                v.to_string(),
            )),
            _ => ResultCellPart::Text(v.to_string()),
        }
    }

    fn from_snak_time(v: &TimeValue) -> Self {
        match ResultCellPart::reduce_time(v) {
            Some((display, year)) => ResultCellPart::Time(display, year),
            None => ResultCellPart::Text("No/unknown value".to_string()),
        }
    }

    fn unit_entity_id_from_url(unit: &str) -> Option<String> {
        if unit == "1" {
            return None;
        }
        unit.rsplit('/')
            .next()
            .filter(|s| s.starts_with('Q') || s.starts_with('P'))
            .map(str::to_string)
    }

    /// Extracts the year from a Wikidata ISO time string like `+1900-00-00T00:00:00Z`.
    /// Returns `None` if the string cannot be parsed.
    pub fn time_sort_year(time_str: &str) -> Option<i32> {
        let s = time_str.strip_prefix('+').unwrap_or(time_str);
        let t_pos = s.find('T')?;
        let date_part = &s[..t_pos];
        let year_str = if let Some(after_sign) = date_part.strip_prefix('-') {
            let dash = after_sign.find('-')?;
            &date_part[..dash + 1]
        } else {
            let dash = date_part.find('-')?;
            &date_part[..dash]
        };
        year_str.parse::<i32>().ok()
    }

    /// Returns `(display_string, sort_year)` for a Wikidata time value.
    pub fn reduce_time(v: &TimeValue) -> Option<(String, i32)> {
        let s = v.time();
        // Parse format: +?(-?\d+)-(\d{1,2})-(\d{1,2})T...
        let s = s.strip_prefix('+').unwrap_or(s);

        let t_pos = s.find('T')?;
        let date_part = &s[..t_pos];

        // Split on '-' but handle negative years (leading '-')
        let (year_str, rest) = if let Some(after_sign) = date_part.strip_prefix('-') {
            // Negative year: find the next '-' after the leading '-'
            let dash = after_sign.find('-')?;
            (&date_part[..dash + 1], &after_sign[dash + 1..])
        } else {
            let dash = date_part.find('-')?;
            (&date_part[..dash], &date_part[dash + 1..])
        };

        let year = year_str.parse::<i32>().ok()?;
        let (month_str, day_str) = rest.split_once('-')?;
        let month = month_str.parse::<u8>().ok()?;
        let day = day_str.parse::<u8>().ok()?;
        let precision_val: u8 = (*v.precision()).try_into().ok()?;
        let precision = Precision::try_from(precision_val).ok()?;

        let display = Era::new(year, month, day, precision).to_string();
        Some((display, year))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- from_sparql_value ---

    #[test]
    fn test_from_sparql_value_entity() {
        let sparql_value = SparqlValue::Entity("Q42".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(
            result,
            ResultCellPart::Entity(EntityInfo::new("Q42".to_string(), true))
        );
    }

    #[test]
    fn test_from_sparql_value_file() {
        let sparql_value = SparqlValue::File("Example.jpg".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(result, ResultCellPart::File("Example.jpg".to_string()));
    }

    #[test]
    fn test_from_sparql_value_uri() {
        let sparql_value = SparqlValue::Uri("http://example.com".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(
            result,
            ResultCellPart::Uri("http://example.com".to_string())
        );
    }

    #[test]
    fn test_from_sparql_value_text() {
        let sparql_value = SparqlValue::Time("2024-01-15".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(result, ResultCellPart::Text("2024-01-15".to_string()));
    }

    #[test]
    fn test_from_sparql_value_literal() {
        let sparql_value = SparqlValue::Literal("Some text".to_string());
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        assert_eq!(result, ResultCellPart::Text("Some text".to_string()));
    }

    #[test]
    fn test_from_sparql_value_location() {
        let sparql_value = SparqlValue::Location(wikimisc::lat_lon::LatLon::new(51.5074, -0.1278));
        let result = ResultCellPart::from_sparql_value(&sparql_value);
        match result {
            ResultCellPart::Location(loc) => {
                assert!((loc.latitude - 51.5074).abs() < 0.0001);
                assert!((loc.longitude - (-0.1278)).abs() < 0.0001);
            }
            other => panic!("Expected Location, got {:?}", other),
        }
    }

    // --- from_snak ---

    #[test]
    fn test_from_snak_entity() {
        let snak = Snak::new_item("P31", "Q5");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::Entity(EntityInfo::new("Q5".to_string(), true))
        );
    }

    #[test]
    fn test_from_snak_string() {
        let snak = Snak::new_string("P1", "hello world");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("hello world".to_string()));
    }

    #[test]
    fn test_from_snak_url() {
        let snak = Snak::new_url("P856", "https://example.com");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::Text("https://example.com".to_string())
        );
    }

    #[test]
    fn test_from_snak_external_id() {
        let snak = Snak::new_external_id("P213", "0000-0001-2345-6789");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::ExternalId(ExternalIdInfo::new(
                "P213".to_string(),
                "0000-0001-2345-6789".to_string()
            ))
        );
    }

    #[test]
    fn test_from_snak_coordinate() {
        let snak = Snak::new_coordinate("P625", 48.8566, 2.3522);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Location(loc) => {
                assert!((loc.latitude - 48.8566).abs() < 0.0001);
                assert!((loc.longitude - 2.3522).abs() < 0.0001);
                assert!(loc.region.is_none());
            }
            other => panic!("Expected Location, got {:?}", other),
        }
    }

    #[test]
    fn test_from_snak_monolingual_text() {
        let snak = Snak::new_monolingual_text("P1476", "en", "Hello World");
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("Hello World:en".to_string()));
    }

    #[test]
    fn test_from_snak_quantity_dimensionless() {
        let snak = Snak::new_quantity("P1082", 42.0);
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Quantity(42.0, None));
    }

    #[test]
    fn test_from_snak_quantity_with_unit() {
        use wikimisc::wikibase::value::QuantityValue;
        use wikimisc::wikibase::{DataValue, DataValueType, Snak, SnakDataType, SnakType, Value};
        let snak = Snak::new(
            SnakDataType::Quantity,
            "P2048",
            SnakType::Value,
            Some(DataValue::new(
                DataValueType::Quantity,
                Value::Quantity(QuantityValue::new(
                    1.96,
                    None,
                    "http://www.wikidata.org/entity/Q11573",
                    None,
                )),
            )),
        );
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(
            part,
            ResultCellPart::Quantity(1.96, Some("Q11573".to_string()))
        );
    }

    #[test]
    fn test_from_snak_no_value() {
        let snak = Snak::new_no_value("P31", SnakDataType::WikibaseItem);
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("No/unknown value".to_string()));
    }

    #[test]
    fn test_from_snak_unknown_value() {
        let snak = Snak::new_unknown_value("P31", SnakDataType::WikibaseItem);
        let part = ResultCellPart::from_snak(&snak);
        assert_eq!(part, ResultCellPart::Text("No/unknown value".to_string()));
    }

    // --- unit_entity_id_from_url ---

    #[test]
    fn test_unit_entity_id_from_url_dimensionless() {
        assert_eq!(ResultCellPart::unit_entity_id_from_url("1"), None);
    }

    #[test]
    fn test_unit_entity_id_from_url_valid_entity() {
        assert_eq!(
            ResultCellPart::unit_entity_id_from_url("http://www.wikidata.org/entity/Q11573"),
            Some("Q11573".to_string())
        );
    }

    #[test]
    fn test_unit_entity_id_from_url_unknown_format() {
        assert_eq!(
            ResultCellPart::unit_entity_id_from_url("https://example.com/unit/foo"),
            None
        );
    }

    // --- reduce_time via from_snak ---

    #[test]
    fn test_from_snak_time_day_precision_produces_time_part() {
        // Precision 11 = day; confirmed working by the sections fixture (1879-03-14)
        let snak = Snak::new_time("P569", "+1879-03-14T00:00:00Z", 11);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "1879-03-14");
                assert_eq!(year, 1879);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_from_snak_time_common_era_day() {
        let snak = Snak::new_time("P569", "+1955-06-08T00:00:00Z", 11);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "1955-06-08");
                assert_eq!(year, 1955);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    // Wikidata renders century-precision dates using the mathematical convention
    // (1st century = years 1-100, 19th century = years 1801-1900, etc.), which
    // matches `era_date`. Issue #145 (comment 4434859636) reported that the bot
    // displayed "20th century" for a Wikidata value that renders on-wiki as
    // "19th century"; verified against the MediaWiki `wbformatvalue` API.

    #[test]
    fn test_reduce_time_century_1900_is_19th_century() {
        // +1900-00-00T00:00:00Z with precision 7 is the LAST year of the 19th century
        // in Wikidata's mathematical convention.
        let snak = Snak::new_time("P569", "+1900-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "19th century");
                assert_eq!(year, 1900);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_1901_is_20th_century() {
        let snak = Snak::new_time("P569", "+1901-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "20th century");
                assert_eq!(year, 1901);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_2000_is_20th_century() {
        let snak = Snak::new_time("P569", "+2000-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "20th century");
                assert_eq!(year, 2000);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_1801_is_19th_century() {
        let snak = Snak::new_time("P569", "+1801-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "19th century");
                assert_eq!(year, 1801);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_1800_is_18th_century() {
        let snak = Snak::new_time("P569", "+1800-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "18th century");
                assert_eq!(year, 1800);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_century_year_33_is_1st_century() {
        // Year 33 with precision 7: original sort issue example from #145.
        let snak = Snak::new_time("P569", "+0033-00-00T00:00:00Z", 7);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, year) => {
                assert_eq!(s, "1st century");
                assert_eq!(year, 33);
            }
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    #[test]
    fn test_reduce_time_decade_1900s() {
        // Issue #44: decade precision should show "1900s" not "190s"
        let snak = Snak::new_time("P569", "+1900-00-00T00:00:00Z", 8);
        let part = ResultCellPart::from_snak(&snak);
        match part {
            ResultCellPart::Time(s, _year) => assert_eq!(s, "1900s"),
            other => panic!("Expected Time, got {:?}", other),
        }
    }

    // --- time_sort_year ---

    #[test]
    fn test_time_sort_year_positive() {
        assert_eq!(
            ResultCellPart::time_sort_year("+1879-03-14T00:00:00Z"),
            Some(1879)
        );
        assert_eq!(
            ResultCellPart::time_sort_year("+1900-00-00T00:00:00Z"),
            Some(1900)
        );
        assert_eq!(
            ResultCellPart::time_sort_year("+0033-00-00T00:00:00Z"),
            Some(33)
        );
    }

    #[test]
    fn test_time_sort_year_negative() {
        assert_eq!(
            ResultCellPart::time_sort_year("-0100-00-00T00:00:00Z"),
            Some(-100)
        );
    }

    #[test]
    fn test_time_sort_year_no_sign() {
        // Without leading '+', year should still parse
        assert_eq!(
            ResultCellPart::time_sort_year("1955-06-08T00:00:00Z"),
            Some(1955)
        );
    }
}
