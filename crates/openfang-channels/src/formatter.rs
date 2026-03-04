//! Channel-specific message formatting.
//!
//! Converts standard Markdown into platform-specific markup:
//! - Telegram HTML: `**bold**` → `<b>bold</b>`
//! - Slack mrkdwn: `**bold**` → `*bold*`, `[text](url)` → `<url|text>`
//! - Plain text: strips all formatting

use openfang_types::config::OutputFormat;

/// Format a message for a specific channel output format.
pub fn format_for_channel(text: &str, format: OutputFormat) -> String {
    match format {
        OutputFormat::Markdown => text.to_string(),
        OutputFormat::DiscordMarkdown => markdown_to_discord(text),
        OutputFormat::TelegramHtml => markdown_to_telegram_html(text),
        OutputFormat::SlackMrkdwn => markdown_to_slack_mrkdwn(text),
        OutputFormat::PlainText => markdown_to_plain(text),
    }
}


/// Convert standard Markdown to Discord-flavored Markdown.
///
/// Discord supports: **bold**, *italic*, __underline__, ~~strikethrough~~,
/// `inline code`, ```code blocks```, > blockquotes, # ## ### headers,
/// - unordered lists, ordered lists, [text](url) links, ||spoilers||.
///
/// Discord does NOT support: Markdown tables, raw HTML tags.
/// This function converts tables into monospace code blocks.
fn markdown_to_discord(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let mut result: Vec<String> = Vec::new();
    let mut i = 0;
    let mut in_code_block = false;

    while i < lines.len() {
        let line = lines[i];

        // Track code blocks (don't modify content inside them)
        if line.trim_start().starts_with("```") {
            in_code_block = !in_code_block;
            result.push(line.to_string());
            i += 1;
            continue;
        }

        if in_code_block {
            result.push(line.to_string());
            i += 1;
            continue;
        }

        // Detect markdown table: line with pipes, next line is separator (|---|---|)
        if line.contains('|') && i + 1 < lines.len() {
            let next = lines[i + 1];
            let stripped = next
                .replace('|', "")
                .replace('-', "")
                .replace(':', "")
                .replace(' ', "");
            let is_table_sep =
                next.contains('|') && next.contains('-') && stripped.is_empty();

            if is_table_sep {
                let header_cells: Vec<&str> = line
                    .split('|')
                    .map(|c| c.trim())
                    .filter(|c| !c.is_empty())
                    .collect();

                i += 2; // skip header + separator

                let mut rows: Vec<Vec<String>> = Vec::new();
                while i < lines.len() && lines[i].contains('|') {
                    let cells: Vec<String> = lines[i]
                        .split('|')
                        .map(|c| c.trim().to_string())
                        .filter(|c| !c.is_empty())
                        .collect();
                    if cells.is_empty() {
                        break;
                    }
                    rows.push(cells);
                    i += 1;
                }

                let num_cols = header_cells.len();
                let mut widths = vec![0usize; num_cols];
                for (j, h) in header_cells.iter().enumerate() {
                    widths[j] = widths[j].max(h.len());
                }
                for row in &rows {
                    for (j, cell) in row.iter().enumerate() {
                        if j < num_cols {
                            widths[j] = widths[j].max(cell.len());
                        }
                    }
                }

                // Render as monospace code block
                result.push("```".to_string());

                let header_line: String = header_cells
                    .iter()
                    .enumerate()
                    .map(|(j, h)| {
                        let w = widths.get(j).copied().unwrap_or(0);
                        format!("{:<w$}", h, w = w)
                    })
                    .collect::<Vec<_>>()
                    .join(" | ");
                result.push(header_line);

                let sep_line: String = widths
                    .iter()
                    .map(|w| "-".repeat(*w))
                    .collect::<Vec<_>>()
                    .join("-+-");
                result.push(sep_line);

                for row in &rows {
                    let data_line: String = (0..num_cols)
                        .map(|j| {
                            let cell = row.get(j).map(|s| s.as_str()).unwrap_or("");
                            let w = widths.get(j).copied().unwrap_or(0);
                            format!("{:<w$}", cell, w = w)
                        })
                        .collect::<Vec<_>>()
                        .join(" | ");
                    result.push(data_line);
                }

                result.push("```".to_string());
                continue;
            }
        }

        // Strip HTML tags Discord doesn't render
        let mut cleaned = line.to_string();
        for tag in &[
            "<br>", "<br/>", "<br />", "<hr>", "<hr/>", "<hr />",
            "<p>", "</p>", "<div>", "</div>", "<span>", "</span>",
        ] {
            cleaned = cleaned.replace(tag, "");
        }

        result.push(cleaned);
        i += 1;
    }

    result.join("\n")
}

/// Convert Markdown to Telegram HTML subset.
///
/// Supported tags: `<b>`, `<i>`, `<code>`, `<pre>`, `<a href="">`.
fn markdown_to_telegram_html(text: &str) -> String {
    // Escape HTML special characters first so agent names and other text
    // don't get interpreted as HTML tags by Telegram's parser.
    let mut result = text
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");

    // Bold: **text** → <b>text</b>
    while let Some(start) = result.find("**") {
        if let Some(end) = result[start + 2..].find("**") {
            let end = start + 2 + end;
            let inner = result[start + 2..end].to_string();
            result = format!("{}<b>{}</b>{}", &result[..start], inner, &result[end + 2..]);
        } else {
            break;
        }
    }

    // Italic: *text* → <i>text</i> (but not inside bold tags)
    // Simple heuristic: match single * not preceded/followed by *
    let mut out = String::with_capacity(result.len());
    let chars: Vec<char> = result.chars().collect();
    let mut i = 0;
    let mut in_italic = false;
    while i < chars.len() {
        if chars[i] == '*'
            && (i == 0 || chars[i - 1] != '*')
            && (i + 1 >= chars.len() || chars[i + 1] != '*')
        {
            if in_italic {
                out.push_str("</i>");
            } else {
                out.push_str("<i>");
            }
            in_italic = !in_italic;
        } else {
            out.push(chars[i]);
        }
        i += 1;
    }
    result = out;

    // Inline code: `text` → <code>text</code>
    while let Some(start) = result.find('`') {
        if let Some(end) = result[start + 1..].find('`') {
            let end = start + 1 + end;
            let inner = result[start + 1..end].to_string();
            result = format!(
                "{}<code>{}</code>{}",
                &result[..start],
                inner,
                &result[end + 1..]
            );
        } else {
            break;
        }
    }

    // Links: [text](url) → <a href="url">text</a>
    while let Some(bracket_start) = result.find('[') {
        if let Some(bracket_end) = result[bracket_start..].find("](") {
            let bracket_end = bracket_start + bracket_end;
            if let Some(paren_end) = result[bracket_end + 2..].find(')') {
                let paren_end = bracket_end + 2 + paren_end;
                let link_text = &result[bracket_start + 1..bracket_end];
                let url = &result[bracket_end + 2..paren_end];
                result = format!(
                    "{}<a href=\"{}\">{}</a>{}",
                    &result[..bracket_start],
                    url,
                    link_text,
                    &result[paren_end + 1..]
                );
            } else {
                break;
            }
        } else {
            break;
        }
    }

    result
}

/// Convert Markdown to Slack mrkdwn format.
fn markdown_to_slack_mrkdwn(text: &str) -> String {
    let mut result = text.to_string();

    // Bold: **text** → *text*
    while let Some(start) = result.find("**") {
        if let Some(end) = result[start + 2..].find("**") {
            let end = start + 2 + end;
            let inner = result[start + 2..end].to_string();
            result = format!("{}*{}*{}", &result[..start], inner, &result[end + 2..]);
        } else {
            break;
        }
    }

    // Links: [text](url) → <url|text>
    while let Some(bracket_start) = result.find('[') {
        if let Some(bracket_end) = result[bracket_start..].find("](") {
            let bracket_end = bracket_start + bracket_end;
            if let Some(paren_end) = result[bracket_end + 2..].find(')') {
                let paren_end = bracket_end + 2 + paren_end;
                let link_text = &result[bracket_start + 1..bracket_end];
                let url = &result[bracket_end + 2..paren_end];
                result = format!(
                    "{}<{}|{}>{}",
                    &result[..bracket_start],
                    url,
                    link_text,
                    &result[paren_end + 1..]
                );
            } else {
                break;
            }
        } else {
            break;
        }
    }

    result
}

/// Strip all Markdown formatting, producing plain text.
fn markdown_to_plain(text: &str) -> String {
    let mut result = text.to_string();

    // Remove bold markers
    result = result.replace("**", "");

    // Remove italic markers (single *)
    // Simple approach: remove isolated *
    let mut out = String::with_capacity(result.len());
    let chars: Vec<char> = result.chars().collect();
    for (i, &ch) in chars.iter().enumerate() {
        if ch == '*'
            && (i == 0 || chars[i - 1] != '*')
            && (i + 1 >= chars.len() || chars[i + 1] != '*')
        {
            continue;
        }
        out.push(ch);
    }
    result = out;

    // Remove inline code markers
    result = result.replace('`', "");

    // Convert links: [text](url) → text (url)
    while let Some(bracket_start) = result.find('[') {
        if let Some(bracket_end) = result[bracket_start..].find("](") {
            let bracket_end = bracket_start + bracket_end;
            if let Some(paren_end) = result[bracket_end + 2..].find(')') {
                let paren_end = bracket_end + 2 + paren_end;
                let link_text = &result[bracket_start + 1..bracket_end];
                let url = &result[bracket_end + 2..paren_end];
                result = format!(
                    "{}{} ({}){}",
                    &result[..bracket_start],
                    link_text,
                    url,
                    &result[paren_end + 1..]
                );
            } else {
                break;
            }
        } else {
            break;
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_markdown_passthrough() {
        let text = "**bold** and *italic*";
        assert_eq!(format_for_channel(text, OutputFormat::Markdown), text);
    }

    #[test]
    fn test_telegram_html_bold() {
        let result = markdown_to_telegram_html("Hello **world**!");
        assert_eq!(result, "Hello <b>world</b>!");
    }

    #[test]
    fn test_telegram_html_italic() {
        let result = markdown_to_telegram_html("Hello *world*!");
        assert_eq!(result, "Hello <i>world</i>!");
    }

    #[test]
    fn test_telegram_html_code() {
        let result = markdown_to_telegram_html("Use `println!`");
        assert_eq!(result, "Use <code>println!</code>");
    }

    #[test]
    fn test_telegram_html_link() {
        let result = markdown_to_telegram_html("[click here](https://example.com)");
        assert_eq!(result, "<a href=\"https://example.com\">click here</a>");
    }

    #[test]
    fn test_slack_mrkdwn_bold() {
        let result = markdown_to_slack_mrkdwn("Hello **world**!");
        assert_eq!(result, "Hello *world*!");
    }

    #[test]
    fn test_slack_mrkdwn_link() {
        let result = markdown_to_slack_mrkdwn("[click](https://example.com)");
        assert_eq!(result, "<https://example.com|click>");
    }

    #[test]
    fn test_plain_text_strips_formatting() {
        let result = markdown_to_plain("**bold** and `code` and *italic*");
        assert_eq!(result, "bold and code and italic");
    }

    #[test]
    fn test_plain_text_converts_links() {
        let result = markdown_to_plain("[click](https://example.com)");
        assert_eq!(result, "click (https://example.com)");
    }
}
