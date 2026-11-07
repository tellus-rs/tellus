use crate::model::{App, Cell, Connection, Entry, Severity, Source, short};
use ratatui::{
    Frame,
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Row, Table, Wrap},
};
use std::time::Duration;
use tellus_cluster_demo::{Phase, ProbeOutcome};

const MIN_WIDTH: u16 = 60;
const MIN_HEIGHT: u16 = 15;
const WIDE: u16 = 100;
const TALL: u16 = 30;

pub fn draw(frame: &mut Frame, app: &App) {
    let area = frame.area();
    if area.width < MIN_WIDTH || area.height < MIN_HEIGHT {
        frame.render_widget(
            Paragraph::new(format!(
                "terminal too small, {MIN_WIDTH}x{MIN_HEIGHT} needed"
            )),
            area,
        );
        return;
    }

    let [strip, body, footer] = Layout::vertical([
        Constraint::Length(4),
        Constraint::Min(1),
        Constraint::Length(1),
    ])
    .areas(area);
    verifier_strip(frame, app, strip);
    self::body(frame, app, body);
    frame.render_widget(
        Line::from(" q quit   ↑/↓ or tab select node   p pause timeline   c clear timeline").dim(),
        footer,
    );
}

fn body(frame: &mut Frame, app: &App, area: Rect) {
    let matrix_height = u16::try_from(app.nodes.len())
        .unwrap_or(u16::MAX)
        .saturating_add(3);
    if area.width >= WIDE {
        let [left, right] =
            Layout::horizontal([Constraint::Percentage(55), Constraint::Percentage(45)])
                .areas(area);
        let [matrix, detail] =
            Layout::vertical([Constraint::Length(matrix_height), Constraint::Min(1)]).areas(left);
        self::matrix(frame, app, matrix);
        self::detail(frame, app, detail);
        timeline(frame, app, right);
    } else if area.height >= TALL {
        let [matrix, detail, timeline] = Layout::vertical([
            Constraint::Length(matrix_height),
            Constraint::Length(15),
            Constraint::Min(1),
        ])
        .areas(area);
        self::matrix(frame, app, matrix);
        self::detail(frame, app, detail);
        self::timeline(frame, app, timeline);
    } else {
        let [matrix, timeline] =
            Layout::vertical([Constraint::Length(matrix_height), Constraint::Min(1)]).areas(area);
        self::matrix(frame, app, matrix);
        self::timeline(frame, app, timeline);
    }
}

fn verifier_strip(frame: &mut Frame, app: &App, area: Rect) {
    let verifier = &app.verifier;
    let chaos = verifier
        .chaos
        .clone()
        .or_else(|| verifier.status.as_ref().map(|status| status.chaos.clone()))
        .unwrap_or_else(|| "unknown".to_string());
    let mut first = vec![
        Span::raw("chaos "),
        Span::styled(chaos.clone(), chaos_style(&chaos)),
    ];
    if let Some(quiet) = verifier
        .status
        .as_ref()
        .and_then(|status| status.quiet_for_millis)
    {
        first.push(Span::raw(format!(
            "   quiet for {:.0?}",
            Duration::from_millis(quiet)
        )));
    }
    let second = match &verifier.status {
        Some(status) => {
            let lb = match status.lb_available {
                Some(true) => Span::styled("available", Style::new().green()),
                Some(false) => Span::styled("unavailable", Style::new().red()),
                None => Span::raw("unknown"),
            };
            vec![
                Span::raw(format!(
                    "verifications {}   mixed {}   violations ",
                    status.verifications, status.mixed_verifications
                )),
                Span::styled(
                    status.violations.to_string(),
                    if status.violations == 0 {
                        Style::new().green()
                    } else {
                        Style::new().red().bold()
                    },
                ),
                Span::raw("   load balancer "),
                lb,
                Span::raw(format!(
                    "   longest outage {:.0?}",
                    Duration::from_millis(
                        u64::try_from(status.longest_lb_outage_millis).unwrap_or(u64::MAX)
                    )
                )),
            ]
        }

        None => vec![Span::raw(format!("no status yet from {}", verifier.url)).dim()],
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .title(Line::from(vec![
            Span::raw(" verifier "),
            connection_span(&verifier.connection),
            Span::raw(" "),
        ]));
    frame.render_widget(
        Paragraph::new(vec![Line::from(first), Line::from(second)]).block(block),
        area,
    );
}

fn matrix(frame: &mut Frame, app: &App, area: Rect) {
    let columns = app.columns();
    let mut header = vec![
        "node".to_string(),
        "phase".to_string(),
        "version".to_string(),
    ];
    header.extend(columns.iter().map(|addr| match app.name_of(*addr) {
        Some(name) => name.to_string(),
        None => addr.to_string(),
    }));

    let rows = app.nodes.iter().enumerate().map(|(index, node)| {
        let stale = node.stale_since.is_some();
        let label = if stale {
            format!("{} (stale)", node.label())
        } else {
            node.label().to_string()
        };
        let mut cells = vec![
            ratatui::widgets::Cell::from(Line::from(vec![
                connection_span(&node.inspect_connection),
                Span::raw(" "),
                Span::raw(label),
            ])),
            ratatui::widgets::Cell::from(phase_span(node.phase)),
            ratatui::widgets::Cell::from(version_span(node.app_version.as_deref())),
        ];
        cells.extend(columns.iter().map(|addr| {
            let span = cell_span(app.cell(index, *addr));
            ratatui::widgets::Cell::from(if stale { span.dim() } else { span })
        }));
        let row = Row::new(cells);
        if index == app.selected {
            row.style(Style::new().add_modifier(Modifier::REVERSED))
        } else {
            row
        }
    });

    let mut widths = vec![
        Constraint::Length(20),
        Constraint::Length(13),
        Constraint::Length(8),
    ];
    widths.extend(columns.iter().map(|_| Constraint::Min(9)));
    let table = Table::new(rows, widths)
        .header(Row::new(header).bold())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(" membership, as each node sees it "),
        );
    frame.render_widget(table, area);
}

fn detail(frame: &mut Frame, app: &App, area: Rect) {
    let node = &app.nodes[app.selected];
    let mut lines = vec![
        Line::from(vec![
            Span::raw("inspect stream "),
            connection_span(&node.inspect_connection),
            Span::raw(format!(" {}", connection_text(&node.inspect_connection))),
            match node.stale_since {
                Some(since) => Span::styled(
                    format!("   view stale since {}", clock(since)),
                    Style::new().yellow(),
                ),
                None => Span::raw(""),
            },
        ]),
        Line::from(vec![
            Span::raw("demo stream    "),
            connection_span(&node.connection),
            Span::raw(format!(
                " {}   {}",
                connection_text(&node.connection),
                node.url
            )),
        ]),
    ];
    match &node.snapshot {
        Some(snapshot) => {
            lines.push(Line::from(vec![
                Span::raw(format!("version {}   phase ", snapshot.version())),
                phase_span(node.phase),
            ]));
            let cluster = snapshot
                .cluster()
                .map_or_else(|| "none yet".to_string(), |cluster| cluster.to_string());
            lines.push(Line::from(format!("cluster {cluster}")));
            let settled = match node.settlement {
                Some(settlement) => {
                    let text = format!(
                        "settled {} at v{}",
                        settlement.settled(),
                        settlement.version()
                    );
                    if settlement.version() == snapshot.version() {
                        Span::raw(text)
                    } else {
                        Span::styled(
                            format!("{text} (state is at v{})", snapshot.version()),
                            Style::new().yellow(),
                        )
                    }
                }

                None => Span::raw("settled ?").dim(),
            };
            lines.push(Line::from(vec![
                Span::raw(format!(
                    "workers {}   subscribers {}   ",
                    count(node.workers),
                    count(node.subscribers)
                )),
                settled,
            ]));
            let members = snapshot
                .members()
                .iter()
                .map(|member| format!("{} {}", short(member), member.state()))
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(Line::from(format!("members {members}")));
            let unreachable = snapshot
                .unreachable()
                .iter()
                .map(short)
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(Line::from(format!("unreachable {unreachable}")));
            let broken_links = snapshot
                .broken_links()
                .iter()
                .map(|link| format!("{} ↛ {}", short(link.observer()), short(link.subject())))
                .collect::<Vec<_>>();
            lines.push(flagged_line(
                "broken links",
                &broken_links,
                Style::new().yellow().bold(),
            ));
            let foreign_clusters = snapshot
                .foreign_clusters()
                .iter()
                .map(|foreign| format!("{} in {}", foreign.addr(), foreign.cluster()))
                .collect::<Vec<_>>();
            lines.push(flagged_line(
                "foreign clusters",
                &foreign_clusters,
                Style::new().red().bold(),
            ));
        }

        None => lines.push(Line::from("no state yet").dim()),
    }
    let publishers = node
        .publishers
        .iter()
        .map(|addr| {
            app.name_of(*addr)
                .map_or_else(|| addr.to_string(), str::to_string)
        })
        .collect::<Vec<_>>()
        .join(", ");
    lines.push(Line::from(format!("events heard from {publishers}")));
    match &node.probes {
        Some(report) => {
            let probes = report
                .probes
                .iter()
                .map(|probe| {
                    let target = app
                        .name_of(probe.addr)
                        .map_or_else(|| probe.addr.to_string(), str::to_string);
                    match &probe.outcome {
                        ProbeOutcome::Ok {
                            millis,
                            app_version: Some(version),
                            ..
                        } => format!("{target} {millis} ms v{version}"),
                        ProbeOutcome::Ok { millis, .. } => format!("{target} {millis} ms"),
                        ProbeOutcome::Failed { error } => format!("{target} failed ({error})"),
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            lines.push(Line::from(format!("probes {probes}")));
        }

        None => lines.push(Line::from("no probe yet").dim()),
    }
    let block = Block::default()
        .borders(Borders::ALL)
        .title(format!(" {} ", node.label()));
    frame.render_widget(
        Paragraph::new(lines)
            .wrap(Wrap { trim: false })
            .block(block),
        area,
    );
}

fn timeline(frame: &mut Frame, app: &App, area: Rect) {
    let entries = app.timeline();
    let visible = usize::from(area.height.saturating_sub(2));
    let skipped = entries.len().saturating_sub(visible);
    let items = entries[skipped..]
        .iter()
        .map(|entry| ListItem::new(entry_line(app, entry)))
        .collect::<Vec<_>>();
    let title = if app.paused() {
        " timeline (paused) "
    } else {
        " timeline "
    };
    frame.render_widget(
        List::new(items).block(Block::default().borders(Borders::ALL).title(title)),
        area,
    );
}

fn entry_line(app: &App, entry: &Entry) -> Line<'static> {
    let source = match entry.source {
        Source::Node(index) => app.nodes[index].label().to_string(),
        Source::Verifier => "verifier".to_string(),
    };
    let style = match entry.severity {
        Severity::Info => Style::new(),
        Severity::Warn => Style::new().yellow(),
        Severity::Error => Style::new().red().bold(),
    };
    Line::from(vec![
        Span::raw(format!("{} ", clock(entry.at))).dim(),
        Span::raw(format!("{source:<9} ")).dim(),
        Span::styled(entry.text.clone(), style),
    ])
}

/// `HH:MM:SS.mmm` in UTC.
fn clock(millis: u64) -> String {
    let seconds = millis / 1_000 % 86_400;
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        seconds / 3_600,
        seconds / 60 % 60,
        seconds % 60,
        millis % 1_000
    )
}

fn count(count: Option<usize>) -> String {
    count.map_or_else(|| "?".to_string(), |count| count.to_string())
}

fn flagged_line(label: &str, items: &[String], style: Style) -> Line<'static> {
    if items.is_empty() {
        return Line::from(format!("{label} none"));
    }

    Line::from(vec![
        Span::raw(format!("{label} ")),
        Span::styled(items.join(", "), style),
    ])
}

fn connection_text(connection: &Connection) -> String {
    match connection {
        Connection::Connecting => "connecting".to_string(),
        Connection::Connected { since } => format!("connected since {}", clock(*since)),
        Connection::Disconnected(reason) => format!("lost: {reason}"),
    }
}

fn connection_span(connection: &Connection) -> Span<'static> {
    match connection {
        Connection::Connecting => Span::styled("◌", Style::new().dim()),
        Connection::Connected { .. } => Span::styled("●", Style::new().green()),
        Connection::Disconnected(_) => Span::styled("○", Style::new().red()),
    }
}

fn phase_span(phase: Option<Phase>) -> Span<'static> {
    match phase {
        Some(Phase::Bootstrapping) => Span::styled("bootstrapping", Style::new().yellow()),
        Some(Phase::Member) => Span::styled("member", Style::new().green()),
        Some(Phase::Downed) => Span::styled("downed", Style::new().red().bold()),
        Some(Phase::Leaving) => Span::styled("leaving", Style::new().yellow()),
        None => Span::styled("?", Style::new().dim()),
    }
}

fn version_span(version: Option<&str>) -> Span<'static> {
    match version {
        Some(version) => Span::raw(version.to_string()),
        None => Span::styled("?", Style::new().dim()),
    }
}

fn cell_span(cell: Cell) -> Span<'static> {
    match cell {
        Cell::Unknown => Span::styled("?", Style::new().dim()),
        Cell::Up => Span::styled("up", Style::new().green()),
        Cell::UpAndDown => Span::styled("up+down", Style::new().magenta()),
        Cell::Left => Span::styled("left", Style::new().blue()),
        Cell::Down => Span::styled("down", Style::new().red()),
        Cell::Unreachable => Span::styled("unreach", Style::new().yellow().bold()),
        Cell::Me => Span::styled("me", Style::new().bold()),
    }
}

fn chaos_style(chaos: &str) -> Style {
    match chaos {
        "quiet" => Style::new().green(),
        "unknown" | "startup" => Style::new().dim(),
        roll if roll.starts_with("upgrade ") || roll.starts_with("downgrade ") => {
            Style::new().fg(Color::Cyan).bold()
        }
        _ => Style::new().fg(Color::Magenta).bold(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        sources::{Input, NodeInput, StreamInput, VerifierInput},
    };
    use ratatui::{Terminal, backend::TestBackend};
    use serde_json::{Value, json};
    use tellus::cluster::ClusterState;
    use tellus_cluster_demo::{NodeEvent, VerifierEvent};
    use tellus_cluster_inspect::{InspectEvent, events::Stamped};

    fn app() -> App {
        app_with(json!([]), json!([]))
    }

    fn app_with(broken_links: Value, foreign_clusters: Value) -> App {
        let config = Config {
            nodes: vec!["http://n1".to_string(), "http://n2".to_string()],
            verifier: "http://v".to_string(),
        };
        let mut app = App::new(&config);
        let addr = "10.0.0.1:7001".parse().expect("valid address");
        let incarnation = "019a0000-0000-7000-8000-000000000001";
        let state = serde_json::from_value::<ClusterState>(json!({
            "version": 1,
            "this": { "addr": addr, "incarnation": incarnation },
            "cluster": "019a0000-0000-7000-8000-0000000000c1",
            "members": [{ "addr": addr, "incarnation": incarnation, "state": "Up" }],
            "unreachable": [],
            "broken_links": broken_links,
            "foreign_clusters": foreign_clusters,
        }))
        .expect("a cluster state deserializes");
        app.apply(
            Input::Node(
                0,
                NodeInput::Stream(StreamInput::Event(Stamped {
                    at: 1,
                    event: NodeEvent::Hello {
                        name: "node1".to_string(),
                        addr,
                        app_version: "1.0.0".to_string(),
                        started_at: 1,
                        resumed: false,
                    },
                })),
            ),
            1,
        );
        app.apply(
            Input::Node(
                0,
                NodeInput::Inspect(StreamInput::Event(Stamped {
                    at: 2,
                    event: InspectEvent::State(state),
                })),
            ),
            2,
        );
        app.apply(
            Input::Verifier(VerifierInput::Stream(StreamInput::Event(Stamped {
                at: 3,
                event: VerifierEvent::Chaos {
                    value: "partition".to_string(),
                },
            }))),
            3,
        );
        app
    }

    fn render(width: u16, height: u16) -> String {
        render_app(&app(), width, height)
    }

    fn render_app(app: &App, width: u16, height: u16) -> String {
        let mut terminal = Terminal::new(TestBackend::new(width, height)).expect("terminal");
        terminal.draw(|frame| draw(frame, app)).expect("drawn");
        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect()
    }

    #[test]
    fn a_wide_terminal_shows_every_panel() {
        let screen = render(120, 40);
        assert!(screen.contains("node1"));
        assert!(screen.contains("version"));
        assert!(screen.contains("1.0.0"));
        assert!(screen.contains("http://n2"));
        assert!(screen.contains("partition"));
        assert!(screen.contains("timeline"));
        assert!(screen.contains("chaos: partition"));
        assert!(screen.contains("10.0.0.1:7001#0001 up"));
    }

    #[test]
    fn the_detail_shows_the_cluster_and_no_split_by_default() {
        let screen = render(120, 40);
        assert!(screen.contains("cluster 019a0000-0000-7000-8000-0000000000c1"));
        assert!(screen.contains("broken links none"));
        assert!(screen.contains("foreign clusters none"));
    }

    #[test]
    fn the_detail_shows_broken_links_and_foreign_clusters() {
        let member = |port: u16, digit: char| {
            json!({
                "addr": format!("10.0.0.{port}:700{port}"),
                "incarnation": format!("019a0000-0000-7000-8000-00000000000{digit}"),
                "state": "Up",
            })
        };
        let app = app_with(
            json!([{ "observer": member(2, '2'), "subject": member(3, '3') }]),
            json!([{
                "addr": "10.0.0.9:7009",
                "cluster": "019a0000-0000-7000-8000-0000000000c2",
            }]),
        );

        let screen = render_app(&app, 200, 40);
        assert!(screen.contains("broken links 10.0.0.2:7002#0002 ↛ 10.0.0.3:7003#0003"));
        assert!(
            screen
                .contains("foreign clusters 10.0.0.9:7009 in 019a0000-0000-7000-8000-0000000000c2")
        );
    }

    #[test]
    fn a_narrow_terminal_still_renders() {
        assert!(render(80, 20).contains("node1"));
        assert!(render(40, 10).contains("too small"));
    }

    #[test]
    fn the_clock_reads_utc() {
        assert_eq!(clock(1_700_000_000_123), "22:13:20.123");
    }
}
