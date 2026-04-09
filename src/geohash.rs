pub const LAT_MIN: f64 = -85.05112878;
pub const LAT_MAX: f64 = 85.05112878;
pub const LON_MIN: f64 = -180.0;
pub const LON_MAX: f64 = 180.0;

const STEP: u8 = 26;

fn spread_u32_to_u64(v: u64) -> u64 {
    let mut result: u64 = v & 0xFFFFFFFF;

    result = (result | (result << 16)) & 0x0000FFFF0000FFFF;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FF;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0F;
    result = (result | (result << 2)) & 0x3333333333333333;
    result = (result | (result << 1)) & 0x5555555555555555;

    result
}

fn interleave(x: u64, y: u64) -> u64 {
    let x = spread_u32_to_u64(x);
    let y = spread_u32_to_u64(y);

    x | (y << 1)
}

pub fn calculate_geohash(lon: f64, lat: f64) -> f64 {
    let lon_offset = (lon - LON_MIN) / (LON_MAX - LON_MIN);
    let lat_offset = (lat - LAT_MIN) / (LAT_MAX - LAT_MIN);

    let lon_bits = (lon_offset * (1u64 << STEP) as f64) as u64;
    let lat_bits = (lat_offset * (1u64 << STEP) as f64) as u64;

    let mut bits: u64 = 0;
    for i in (0..STEP).rev() {
        bits = (bits << 1) | ((lon_bits >> i) & 1);
        bits = (bits << 1) | ((lat_bits >> i) & 1);
    }
    bits as f64
}

#[cfg(test)]
mod test {
    use super::*;

    struct TestCase {
        name: &'static str,
        latitude: f64,
        longitude: f64,
        expected_score: u64,
    }

    #[test]
    fn test_geohash() {
        let test_cases = vec![
            TestCase {
                name: "Bangkok",
                latitude: 13.7220,
                longitude: 100.5252,
                expected_score: 3962257306574459,
            },
            TestCase {
                name: "Beijing",
                latitude: 39.9075,
                longitude: 116.3972,
                expected_score: 4069885364908765,
            },
            TestCase {
                name: "Berlin",
                latitude: 52.5244,
                longitude: 13.4105,
                expected_score: 3673983964876493,
            },
            TestCase {
                name: "Copenhagen",
                latitude: 55.6759,
                longitude: 12.5655,
                expected_score: 3685973395504349,
            },
            TestCase {
                name: "New Delhi",
                latitude: 28.6667,
                longitude: 77.2167,
                expected_score: 3631527070936756,
            },
            TestCase {
                name: "Kathmandu",
                latitude: 27.7017,
                longitude: 85.3206,
                expected_score: 3639507404773204,
            },
            TestCase {
                name: "London",
                latitude: 51.5074,
                longitude: -0.1278,
                expected_score: 2163557714755072,
            },
            TestCase {
                name: "New York",
                latitude: 40.7128,
                longitude: -74.0060,
                expected_score: 1791873974549446,
            },
            TestCase {
                name: "Paris",
                latitude: 48.8534,
                longitude: 2.3488,
                expected_score: 3663832752681684,
            },
            TestCase {
                name: "Sydney",
                latitude: -33.8688,
                longitude: 151.2093,
                expected_score: 3252046221964352,
            },
            TestCase {
                name: "Tokyo",
                latitude: 35.6895,
                longitude: 139.6917,
                expected_score: 4171231230197045,
            },
            TestCase {
                name: "Vienna",
                latitude: 48.2064,
                longitude: 16.3707,
                expected_score: 3673109836391743,
            },
        ];

        for test_case in test_cases {
            let actual_score = calculate_geohash(test_case.latitude, test_case.longitude);
            let success = actual_score as u64 == test_case.expected_score;
            let status = if success { "✅" } else { "❌" };
            println!("{}: {} ({})", test_case.name, actual_score, status);
        }
    }
}
