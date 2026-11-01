/// A server-sent events parser; an `id` field applies to every later message until the next one.
#[derive(Debug, Default)]
pub struct SseParser {
    buffer: Vec<u8>,
    data: String,
    last_id: Option<String>,
}

impl SseParser {
    pub fn push(&mut self, bytes: &[u8]) -> Vec<SseMessage> {
        self.buffer.extend_from_slice(bytes);

        let mut messages = Vec::new();
        while let Some(end) = self.buffer.iter().position(|byte| *byte == b'\n') {
            let line = self.buffer.drain(..=end).collect::<Vec<_>>();
            let line = String::from_utf8_lossy(&line);
            let line = line.trim_end_matches(['\n', '\r']);
            if let Some(message) = self.line(line) {
                messages.push(message);
            }
        }

        messages
    }

    fn line(&mut self, line: &str) -> Option<SseMessage> {
        if line.is_empty() {
            if self.data.is_empty() {
                return None;
            }
            let data = std::mem::take(&mut self.data);
            return Some(SseMessage {
                id: self.last_id.clone(),
                data,
            });
        }
        if line.starts_with(':') {
            return None;
        }

        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "data" => {
                if !self.data.is_empty() {
                    self.data.push('\n');
                }
                self.data.push_str(value);
            }

            "id" => self.last_id = Some(value.to_string()),

            _ => {}
        }

        None
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SseMessage {
    pub id: Option<String>,
    pub data: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(id: Option<&str>, data: &str) -> SseMessage {
        SseMessage {
            id: id.map(str::to_string),
            data: data.to_string(),
        }
    }

    #[test]
    fn a_message_split_across_chunks_arrives_once_complete() {
        let mut parser = SseParser::default();
        assert!(parser.push(b"data: {\"a\":").is_empty());
        assert!(parser.push(b"1}\nid: 7:1\n").is_empty());
        assert_eq!(parser.push(b"\n"), vec![message(Some("7:1"), "{\"a\":1}")]);
    }

    #[test]
    fn crlf_line_endings_are_accepted() {
        let mut parser = SseParser::default();
        assert_eq!(
            parser.push(b"data: x\r\nid: 1:1\r\n\r\n"),
            vec![message(Some("1:1"), "x")]
        );
    }

    #[test]
    fn multi_line_data_is_joined_with_newlines() {
        let mut parser = SseParser::default();
        assert_eq!(
            parser.push(b"data: one\ndata: two\n\n"),
            vec![message(None, "one\ntwo")]
        );
    }

    #[test]
    fn comments_and_unknown_fields_are_skipped() {
        let mut parser = SseParser::default();
        assert!(parser.push(b": keep-alive\n\n").is_empty());
        assert_eq!(
            parser.push(b"event: ignored\nretry: 5\ndata: x\n\n"),
            vec![message(None, "x")]
        );
    }

    #[test]
    fn the_id_persists_across_messages() {
        let mut parser = SseParser::default();
        let messages = parser.push(b"data: a\nid: 1:1\n\ndata: b\n\n");
        assert_eq!(
            messages,
            vec![message(Some("1:1"), "a"), message(Some("1:1"), "b")]
        );
    }
}
