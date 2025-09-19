//! src/error.rs

pub fn error_chain_fmt(
    f: &mut std::fmt::Formatter<'_>,
    e: &impl std::error::Error,
) -> std::fmt::Result {
    writeln!(f, "{}\n", e)?;
    let mut current = e.source();
    while let Some(cause) = current {
        writeln!(f, "Caused by:\n\t{}", cause)?;
        current = cause.source();
    }
    Ok(())
}
