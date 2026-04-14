use separator::Separatable;

pub trait PreciseSeparator {
    fn separate_with_precision(&self, places: usize) -> String;
}

impl PreciseSeparator for f64 {
    fn separate_with_precision(&self, places: usize) -> String {
        // 1. Handle the integer part with commas
        let integer_part = self.trunc() as i64;
        let formatted_int = integer_part.separated_string();

        // 2. Handle the fractional part
        // We format it to the desired precision (e.g., "0.005")
        let fract_str = format!("{:.1$}", self.fract().abs(), places);

        // 3. Remove the leading '0' from "0.005" to get ".005"
        let decimal_part = &fract_str[1..];

        format!("{}{}", formatted_int, decimal_part)
    }
}
