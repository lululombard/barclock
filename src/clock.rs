//! Wall-clock text: the big local time, the date line and the extra time zones.
//! Pure functions so they can be unit-tested without a display.

use std::time::Duration;

use chrono::{DateTime, Datelike, Timelike, Utc, Weekday};
use chrono_tz::Tz;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClockText {
    /// `HH:MM:SS`
    pub time: String,
    /// `dd/mm/yyyy`
    pub date: String,
    /// Localized weekday name, capitalized.
    pub weekday: String,
    /// `(label, HH:MM)` for every configured extra zone, in configuration order.
    pub zones: Vec<(String, String)>,
}

pub fn render(now: DateTime<Utc>, tz: Tz, zones: &[(String, Tz)], lang: &str) -> ClockText {
    let local = now.with_timezone(&tz);
    ClockText {
        time: format!("{:02}:{:02}:{:02}", local.hour(), local.minute(), local.second()),
        date: format!("{:02}/{:02}/{:04}", local.day(), local.month(), local.year()),
        weekday: weekday_name(local.weekday(), lang).to_string(),
        zones: zones
            .iter()
            .map(|(label, z)| {
                let t = now.with_timezone(z);
                (label.clone(), format!("{:02}:{:02}", t.hour(), t.minute()))
            })
            .collect(),
    }
}

/// Time until the next whole second, plus a small guard so the tick lands just after it.
pub fn until_next_second(now: DateTime<Utc>) -> Duration {
    let nanos = u64::from(now.timestamp_subsec_nanos());
    Duration::from_nanos(1_000_000_000u64.saturating_sub(nanos) + 5_000_000)
}

pub fn weekday_name(day: Weekday, lang: &str) -> &'static str {
    match (lang, day) {
        ("fr", Weekday::Mon) => "Lundi",
        ("fr", Weekday::Tue) => "Mardi",
        ("fr", Weekday::Wed) => "Mercredi",
        ("fr", Weekday::Thu) => "Jeudi",
        ("fr", Weekday::Fri) => "Vendredi",
        ("fr", Weekday::Sat) => "Samedi",
        ("fr", Weekday::Sun) => "Dimanche",
        (_, Weekday::Mon) => "Monday",
        (_, Weekday::Tue) => "Tuesday",
        (_, Weekday::Wed) => "Wednesday",
        (_, Weekday::Thu) => "Thursday",
        (_, Weekday::Fri) => "Friday",
        (_, Weekday::Sat) => "Saturday",
        (_, Weekday::Sun) => "Sunday",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn renders_toronto_and_zones() {
        // 2026-09-05 19:04:09 UTC = 15:04:09 in Toronto (EDT), 21:04 Paris, 20:04 London.
        let now = Utc.with_ymd_and_hms(2026, 9, 5, 19, 4, 9).unwrap();
        let zones = vec![
            ("Paris".to_string(), chrono_tz::Europe::Paris),
            ("London".to_string(), chrono_tz::Europe::London),
            ("UTC".to_string(), chrono_tz::UTC),
        ];
        let t = render(now, chrono_tz::America::Toronto, &zones, "fr");
        assert_eq!(t.time, "15:04:09");
        assert_eq!(t.date, "05/09/2026");
        assert_eq!(t.weekday, "Samedi");
        assert_eq!(
            t.zones,
            vec![
                ("Paris".to_string(), "21:04".to_string()),
                ("London".to_string(), "20:04".to_string()),
                ("UTC".to_string(), "19:04".to_string()),
            ]
        );
        assert_eq!(render(now, chrono_tz::America::Toronto, &[], "en").weekday, "Saturday");
    }

    #[test]
    fn next_second_guard() {
        let now = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap() + chrono::Duration::milliseconds(250);
        let d = until_next_second(now);
        assert!(d > Duration::from_millis(750) && d < Duration::from_millis(760), "{d:?}");
    }
}
