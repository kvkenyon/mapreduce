//! src/telemetry.rs
use opentelemetry::trace::TracerProvider as _;
use tracing_subscriber::{fmt::format::FmtSpan, prelude::*};

pub fn init_tracing(
    service_name: &'static str,
) -> anyhow::Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_resource(
            opentelemetry_sdk::Resource::builder()
                .with_service_name(service_name)
                .build(),
        )
        .with_batch_exporter(
            opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .build()?,
        )
        .build();
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    let tracer = tracer_provider.tracer(service_name);

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::from_default_env())
        .with(tracing_subscriber::fmt::layer().with_span_events(FmtSpan::NEW | FmtSpan::CLOSE))
        .with(tracing_opentelemetry::layer().with_tracer(tracer))
        .try_init()?;

    Ok(tracer_provider)
}
