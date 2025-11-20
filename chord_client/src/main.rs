use chord_proto::chord::chord_client::ChordClient;
use chord_proto::chord::{GetRequest, PutRequest};
use clap::{Parser, Subcommand};
use tonic::Request;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Address of the node to connect to
    #[arg(short, long, default_value = "http://127.0.0.1:5000")]
    node: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Put a key-value pair into the DHT
    Put { key: String, value: String },
    /// Get a value from the DHT
    Get { key: String },
    /// Find successor of an ID
    FindSuccessor { id: u64 },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    let mut client = ChordClient::connect(cli.node).await?;

    match cli.command {
        Commands::Put { key, value } => {
            let request = Request::new(PutRequest { key, value });
            let response = client.put(request).await?;
            if response.into_inner().success {
                println!("Put successful");
            } else {
                println!("Put failed");
            }
        }
        Commands::Get { key } => {
            let request = Request::new(GetRequest { key });
            let response = client.get(request).await?;
            let resp = response.into_inner();
            if resp.found {
                println!("Value: {}", resp.value);
            } else {
                println!("Key not found");
            }
        }
        Commands::FindSuccessor { id } => {
            let request = Request::new(chord_proto::chord::FindSuccessorRequest { id });
            let response = client.find_successor(request).await?;
            let node = response.into_inner();
            println!("Successor: ID={}, Address={}", node.id, node.address);
        }
    }

    Ok(())
}
