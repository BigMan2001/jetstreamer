use jetstreamer::JetstreamerRunner;
use std::{collections::HashSet, path::Path};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proxies: Option<&Path> = None; // or: Some(Path::new("proxies.txt"))

    JetstreamerRunner::default()
        .with_log_level("info")
        .parse_cli_args()?
        .run(HashSet::new(), proxies)
        .map_err(|err| -> Box<dyn std::error::Error> { Box::new(err) })?;

    Ok(())
}
