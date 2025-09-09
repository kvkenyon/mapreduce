//! src/main.rs
use mapreduce::configuration::get_configuration;

fn main() {
    let configuration = get_configuration().expect("Failed to read configuration.");
    println!("{:?}", configuration);
}
