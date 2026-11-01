use anyhow::{Context, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cmp::Reverse,
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
    process::Command,
    thread,
};

const PACKAGES: [&str; 3] = ["kameo", "ractor", "tellus"];
const LATEST: &str = "latest";
const COMPARISON_URL: &str = "https://tellus.rs/tellus/comparison/";
const HEADER: &str = r#"<header class="top"><div class="wrap">
<a class="brand" href="https://tellus.rs/" aria-label="tellus home"><svg viewBox="0 0 32 32" aria-hidden="true"><path d="M2.33 13h27.35" stroke="currentColor" stroke-width="2.5"/><path d="M2.44 19.5h27.11M5.45 25.2h21.11" stroke="var(--accent)" stroke-width="2.5"/><circle cx="16" cy="16" r="14" fill="none" stroke="currentColor" stroke-width="2.5"/></svg>tellus</a>
<nav class="nav" aria-label="Main"><a href="https://tellus.rs/docs/">Docs</a><a href="https://tellus.rs/tellus/comparison/" aria-current="page">Comparison</a><a href="https://tellus.rs/tellus/dev/bench/">Regression benchmarks</a><a href="https://github.com/tellus-rs/tellus">GitHub</a></nav>
</div></header>"#;

fn main() -> anyhow::Result<()> {
    let args = Args::from_env()?;

    if let Some(directory) = &args.index {
        return write_index(directory);
    }

    let report = match &args.from_json {
        Some(path) => read_report(path)?,

        None => {
            let measurements = read_measurements(&args.input)?;
            if measurements.is_empty() {
                bail!("no criterion results below {}", args.input.display());
            }
            Report {
                metadata: read_metadata(args.tag)?,
                measurements,
            }
        }
    };

    fs::create_dir_all(&args.output)
        .with_context(|| format!("output directory {}", args.output.display()))?;

    // Rewriting a read results.json would change the last digit of some floats.
    if args.from_json.is_none() {
        let json = serde_json::to_string_pretty(&report).context("report as JSON")?;
        fs::write(args.output.join("results.json"), json).context("results.json")?;
    }
    fs::write(args.output.join("index.html"), render_html(&report)).context("index.html")?;

    println!(
        "wrote {} measurements for {} to {}",
        report.measurements.len(),
        report.metadata.tag,
        args.output.display()
    );

    Ok(())
}

struct Args {
    input: PathBuf,
    output: PathBuf,
    tag: String,
    from_json: Option<PathBuf>,
    index: Option<PathBuf>,
}

impl Args {
    fn from_env() -> anyhow::Result<Self> {
        let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");

        let mut input = workspace.join("target/criterion-comparison");
        let mut output = workspace.join("target/comparison-report");
        let mut tag = env::var("GITHUB_REF_NAME").unwrap_or_else(|_| "local".to_string());
        let mut from_json = None;
        let mut index = None;

        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            let mut value = || args.next().with_context(|| format!("value for {arg}"));

            match arg.as_str() {
                "--input" => input = PathBuf::from(value()?),
                "--output" => output = PathBuf::from(value()?),
                "--tag" => tag = value()?,
                "--from-json" => from_json = Some(PathBuf::from(value()?)),
                "--index" => index = Some(PathBuf::from(value()?)),
                _ => bail!("unexpected argument {arg}"),
            }
        }

        Ok(Args {
            input,
            output,
            tag,
            from_json,
            index,
        })
    }
}

#[derive(Serialize, Deserialize)]
struct Report {
    metadata: Metadata,
    measurements: Vec<Measurement>,
}

#[derive(Serialize, Deserialize)]
struct Metadata {
    tag: String,
    commit: Option<String>,
    date: Option<String>,
    os: Option<String>,
    cpu: Option<String>,
    cores: Option<usize>,
    rustc: Option<String>,
    versions: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
struct Measurement {
    group: String,
    framework: String,
    parameter: Option<String>,
    elements: u64,
    mean_ns: f64,
    lower_ns: f64,
    upper_ns: f64,
    elements_per_second: f64,
}

impl Measurement {
    fn key(&self) -> (&str, Option<&str>) {
        (&self.group, self.parameter.as_deref())
    }
}

fn write_index(directory: &Path) -> anyhow::Result<()> {
    let mut runs = fs::read_dir(directory)
        .with_context(|| format!("reading directory {}", directory.display()))?
        .map(|entry| {
            let path = entry
                .with_context(|| format!("entry below {}", directory.display()))?
                .path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .with_context(|| format!("name of {}", path.display()))?
                .to_string();
            Ok((name, path))
        })
        .collect::<anyhow::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(name, path)| name != LATEST && path.join("results.json").is_file())
        .map(|(name, path)| Ok((name, read_report(&path.join("results.json"))?.metadata)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    runs.sort_by(|(a, _), (b, _)| run_order(a).cmp(&run_order(b)));

    let latest_path = directory.join(LATEST).join("results.json");
    let latest = if latest_path.is_file() {
        Some(read_report(&latest_path)?.metadata)
    } else {
        None
    };

    let latest_item = latest
        .map(|metadata| {
            format!(
                r#"<li><a href="./{LATEST}/"><span class="latest">latest</span><span class="note">{tag}, {date}</span></a></li>"#,
                tag = escape(&metadata.tag),
                date = escape(day(metadata.date.as_deref())),
            )
        })
        .unwrap_or_default();
    let items = runs
        .iter()
        .map(|(name, metadata)| {
            format!(
                r#"<li><a href="./{name}/"><span>{name}</span><span class="note">{date}</span></a></li>"#,
                name = escape(name),
                date = escape(day(metadata.date.as_deref())),
            )
        })
        .collect::<String>();

    let description =
        "Messaging throughput of tellus against kameo and ractor, one run per release.";
    let body = format!(
        r#"<p class="eyebrow">Benchmarks</p>
<h1>tellus comparison benchmarks</h1>
<p class="lede">{description}</p>
<ul class="runs">{latest_item}{items}</ul>
<div class="foot"><span>See the <a href="../dev/bench/">regression benchmarks</a> for tellus's own throughput over time.</span></div>
"#
    );

    let path = directory.join("index.html");
    fs::write(
        &path,
        page(
            "tellus comparison benchmarks",
            description,
            COMPARISON_URL,
            &body,
        ),
    )
    .with_context(|| format!("writing {}", path.display()))
}

fn read_report(path: &Path) -> anyhow::Result<Report> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&contents).with_context(|| format!("parsing {}", path.display()))
}

fn run_order(name: &str) -> RunOrder<'_> {
    let version = name.strip_prefix("tellus-v").and_then(|version| {
        version
            .split('.')
            .map(|part| part.parse::<u64>().ok())
            .collect::<Option<Vec<_>>>()
    });

    match version {
        Some(version) => RunOrder::Release(Reverse(version)),
        None => RunOrder::Other(Reverse(name)),
    }
}

// Derived `Ord` compares by variant first, hence releases sort before other runs.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum RunOrder<'a> {
    Release(Reverse<Vec<u64>>),
    Other(Reverse<&'a str>),
}

fn day(date: Option<&str>) -> &str {
    date.and_then(|date| date.get(..10))
        .unwrap_or("unknown date")
}

fn read_measurements(input: &Path) -> anyhow::Result<Vec<Measurement>> {
    let directories = collect_sample_directories(input)
        .with_context(|| format!("walking {}", input.display()))?;

    let mut measurements = directories
        .iter()
        .map(|directory| read_measurement(directory))
        .collect::<anyhow::Result<Vec<_>>>()?;

    measurements.sort_by(|a, b| {
        a.group
            .cmp(&b.group)
            .then_with(|| parameter_order(a).cmp(&parameter_order(b)))
            .then_with(|| a.framework.cmp(&b.framework))
    });

    Ok(measurements)
}

fn collect_sample_directories(directory: &Path) -> anyhow::Result<Vec<PathBuf>> {
    if !directory.is_dir() {
        return Ok(Vec::new());
    }

    let collected = fs::read_dir(directory)
        .with_context(|| format!("reading directory {}", directory.display()))?
        .map(|entry| {
            let path = entry
                .with_context(|| format!("entry below {}", directory.display()))?
                .path();
            if !path.is_dir() {
                Ok(Vec::new())
            } else if path.file_name().and_then(|name| name.to_str()) == Some("new")
                && path.join("benchmark.json").is_file()
                && path.join("estimates.json").is_file()
            {
                Ok(vec![path])
            } else {
                collect_sample_directories(&path)
            }
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    Ok(collected.into_iter().flatten().collect())
}

fn read_measurement(directory: &Path) -> anyhow::Result<Measurement> {
    let benchmark = read_json(&directory.join("benchmark.json"))?;
    let estimates = read_json(&directory.join("estimates.json"))?;

    let group = string(&benchmark, "/group_id")?;
    let framework = string(&benchmark, "/function_id")?;
    let parameter = benchmark
        .pointer("/value_str")
        .and_then(Value::as_str)
        .map(ToString::to_string);

    let elements = count(&benchmark, "/throughput/Elements")
        .with_context(|| format!("element throughput of {group}/{framework}"))?;

    let mean_ns = number(&estimates, "/mean/point_estimate")?;
    let lower_ns = number(&estimates, "/mean/confidence_interval/lower_bound")?;
    let upper_ns = number(&estimates, "/mean/confidence_interval/upper_bound")?;

    Ok(Measurement {
        group,
        framework,
        parameter,
        elements,
        mean_ns,
        lower_ns,
        upper_ns,
        elements_per_second: elements as f64 * 1e9 / mean_ns,
    })
}

fn read_json(path: &Path) -> anyhow::Result<Value> {
    let contents =
        fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str(&contents).with_context(|| format!("parsing {}", path.display()))
}

fn string(value: &Value, pointer: &str) -> anyhow::Result<String> {
    value
        .pointer(pointer)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .with_context(|| format!("string field {pointer}"))
}

fn count(value: &Value, pointer: &str) -> anyhow::Result<u64> {
    value
        .pointer(pointer)
        .and_then(Value::as_u64)
        .with_context(|| format!("integer field {pointer}"))
}

fn number(value: &Value, pointer: &str) -> anyhow::Result<f64> {
    value
        .pointer(pointer)
        .and_then(Value::as_f64)
        .with_context(|| format!("number field {pointer}"))
}

fn parameter_order(measurement: &Measurement) -> ParameterOrder<'_> {
    match &measurement.parameter {
        Some(parameter) => match parameter.parse() {
            Ok(number) => ParameterOrder::Number(number),
            Err(_) => ParameterOrder::Text(parameter),
        },
        None => ParameterOrder::Missing,
    }
}

/// Derived `Ord` compares by variant first, hence the declaration order is the sort order.
#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum ParameterOrder<'a> {
    Missing,
    Number(u64),
    Text(&'a str),
}

fn read_metadata(tag: String) -> anyhow::Result<Metadata> {
    Ok(Metadata {
        tag,
        commit: or_none(env::var("GITHUB_SHA").or_else(|_| run("git", &["rev-parse", "HEAD"]))),
        date: or_none(run("date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"])),
        os: or_none(run("uname", &["-sr"])),
        cpu: read_cpu(),
        cores: thread::available_parallelism()
            .map(|cores| cores.get())
            .ok(),
        rustc: or_none(run("rustc", &["--version"])),
        versions: read_versions()?,
    })
}

fn read_cpu() -> Option<String> {
    let from_cpuinfo = fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|cpuinfo| model_name(&cpuinfo));

    from_cpuinfo.or_else(|| {
        or_none(
            run("lscpu", &[])
                .and_then(|lscpu| model_name(&lscpu).context("model name in lscpu output")),
        )
    })
}

fn model_name(text: &str) -> Option<String> {
    text.lines()
        .find(|line| line.to_lowercase().starts_with("model name"))
        .and_then(|line| line.split_once(':'))
        .map(|(_, model)| model.trim().to_string())
        .filter(|model| !model.is_empty() && model != "-")
}

fn read_versions() -> anyhow::Result<BTreeMap<String, String>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../Cargo.lock");
    let lock = fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;

    let mut versions = BTreeMap::new();
    let mut name = None;
    for line in lock.lines() {
        if let Some(value) = line.strip_prefix("name = ") {
            name = Some(value.trim_matches('"').to_string());
        } else if let Some(value) = line.strip_prefix("version = ")
            && let Some(name) = name.take()
            && PACKAGES.contains(&name.as_str())
        {
            versions.insert(name, value.trim_matches('"').to_string());
        }
    }

    let missing = PACKAGES
        .iter()
        .filter(|package| !versions.contains_key(**package))
        .copied()
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        bail!(
            "no version in {} for {}",
            path.display(),
            missing.join(", ")
        );
    }

    Ok(versions)
}

/// Keep the caveats below in sync with the ones in the README.
fn render_html(report: &Report) -> String {
    let sections = report
        .measurements
        .chunk_by(|a, b| a.key() == b.key())
        .map(|measurements| {
            let (group, parameter) = measurements[0].key();
            render_section(group, parameter, measurements)
        })
        .collect::<String>();

    let metadata = &report.metadata;
    let versions = metadata
        .versions
        .iter()
        .map(|(name, version)| format!("{name} {version}"))
        .collect::<Vec<_>>()
        .join(", ");

    let body = format!(
        r#"<p class="eyebrow">Comparison benchmarks, {tag}</p>
<h1>tellus against kameo and ractor</h1>
<p class="lede">Messaging throughput, higher is better.</p>

<dl class="meta">
<dt>Tag</dt><dd>{tag}</dd>
<dt>Commit</dt><dd>{commit}</dd>
<dt>Date</dt><dd>{date}</dd>
<dt>Versions</dt><dd>{versions}</dd>
<dt>CPU</dt><dd>{cpu} ({cores} cores)</dd>
<dt>OS</dt><dd>{os}</dd>
<dt>Toolchain</dt><dd>{rustc}</dd>
</dl>

<div class="caveats">
<h2>Read this before drawing conclusions</h2>
<ol>
<li><strong>tellus's <code>receive</code> is synchronous; kameo's and ractor's handlers are <code>async fn</code>.</strong>
tellus therefore avoids allocating and polling a future per message, but cannot await inside <code>receive</code>.
This is a capability difference, not only a speed difference, and it favors tellus on exactly these microbenchmarks.</li>
<li><strong>tellus's mailbox is statically typed; the others erase message types</strong>, costing an allocation
and a dynamic dispatch per message that tellus does not pay.</li>
<li><strong>Competitors are configured for speed, not defaults.</strong> kameo runs without <code>tracing</code>
and ractor without <code>message_span_propogation</code>, both of which add per-message instrumentation that
tellus has no equivalent of. This biases the setup in the competitors' favor.</li>
<li><strong>Messaging microbenchmarks only.</strong> Nothing here speaks to supervision, distribution,
ergonomics, memory use or production readiness. kameo and ractor are mature, feature-rich frameworks; tellus is
under active development and does far less.</li>
<li><strong>On CI these run on a shared runner</strong>, so absolute figures are not representative of real
deployments. Only the relative comparison within a single run is meaningful.</li>
<li><strong>Written and run by tellus's maintainer.</strong> The full methodology and benchmark source are in the
repository; corrections are welcome.</li>
</ol>
</div>
{sections}
<div class="foot"><span>Generated from criterion results.</span><span><a href="../">All runs</a> &middot; <a href="../../dev/bench/">Regression benchmarks</a></span></div>
"#,
        tag = escape(&metadata.tag),
        commit = escape(or_unknown(metadata.commit.as_deref())),
        date = escape(or_unknown(metadata.date.as_deref())),
        versions = escape(&versions),
        cpu = escape(or_unknown(metadata.cpu.as_deref())),
        cores = metadata
            .cores
            .map_or_else(|| "unknown".to_string(), |cores| cores.to_string()),
        os = escape(or_unknown(metadata.os.as_deref())),
        rustc = escape(or_unknown(metadata.rustc.as_deref())),
    );

    page(
        &format!("tellus comparison benchmarks ({})", escape(&metadata.tag)),
        &format!(
            "Messaging throughput of tellus against kameo and ractor for {}, with the methodology and its caveats.",
            escape(&metadata.tag)
        ),
        &format!("{COMPARISON_URL}{LATEST}/"),
        &body,
    )
}

fn render_section(group: &str, parameter: Option<&str>, measurements: &[Measurement]) -> String {
    let fastest = measurements
        .iter()
        .map(|measurement| measurement.elements_per_second)
        .fold(0.0, f64::max);

    let heading = match parameter {
        Some(parameter) => format!("{group} ({parameter})"),
        None => group.to_string(),
    };

    let rows = measurements
        .iter()
        .map(|measurement| {
            let relative = if fastest > 0.0 {
                measurement.elements_per_second / fastest
            } else {
                0.0
            };
            let width = relative * 100.0;

            format!(
                r#"<tr{own}><td>{framework}</td><td class="num">{mean:.3} ms</td><td class="num">{throughput:.2} M/s</td><td class="num">{relative:.2}x</td><td class="barcell"><span class="bar" style="width:{width:.1}%"></span></td></tr>"#,
                own = if measurement.framework == "tellus" {
                    r#" class="own""#
                } else {
                    ""
                },
                framework = escape(&measurement.framework),
                mean = measurement.mean_ns / 1e6,
                throughput = measurement.elements_per_second / 1e6,
            )
        })
        .collect::<String>();

    format!(
        r#"<h2>{heading}</h2>
<div class="scroll"><table>
<thead><tr><th>Framework</th><th class="num">Time</th><th class="num">Throughput</th><th class="num">vs fastest</th><th></th></tr></thead>
<tbody>{rows}</tbody>
</table></div>
"#,
        heading = escape(&heading),
    )
}

fn page(title: &str, description: &str, canonical: &str, body: &str) -> String {
    format!(
        r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<meta name="description" content="{description}">
<link rel="canonical" href="{canonical}">
<link rel="icon" href="https://tellus.rs/favicon.svg" type="image/svg+xml">
<link rel="stylesheet" href="https://tellus.rs/dashboards.css">
</head>
<body>
{HEADER}
<main class="wrap">
{body}</main>
</body>
</html>
"#
    )
}

fn run(program: &str, args: &[&str]) -> anyhow::Result<String> {
    let output = Command::new(program)
        .args(args)
        .output()
        .with_context(|| format!("running {program}"))?;
    if !output.status.success() {
        bail!("{program} exited with {}", output.status);
    }

    let stdout =
        String::from_utf8(output.stdout).with_context(|| format!("{program} stdout as UTF-8"))?;

    Ok(stdout.trim().to_string())
}

fn or_none(value: anyhow::Result<String>) -> Option<String> {
    value
        .inspect_err(|error| eprintln!("metadata unavailable: {error:#}"))
        .ok()
}

fn or_unknown(value: Option<&str>) -> &str {
    value.unwrap_or("unknown")
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}
