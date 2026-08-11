use std::net::SocketAddr;

use anyhow::{Context, anyhow, bail};
use aster_core::Value;
use aster_protocol::{
    ClientOperation, ClientRequest, DEFAULT_MAX_FRAME_BYTES, ReadConsistency, ResponseResult,
    SessionRequest, WireMessage, read_message, write_message,
};
use clap::{Parser, Subcommand};
use tokio::net::TcpStream;

#[derive(Debug, Parser)]
#[command(name = "aster", about = "AsterDB command-line client")]
struct Arguments {
    #[arg(long, default_value = "127.0.0.1:7442")]
    address: SocketAddr,
    /// Stable 16-byte client identifier as 32 hexadecimal digits. Supplying
    /// this with `--sequence` makes an autocommit retry externally replayable.
    #[arg(long, value_parser = parse_client_id)]
    client_id: Option<[u8; 16]>,
    /// Positive logical mutation sequence for this client identifier.
    #[arg(long, default_value_t = 1, value_parser = parse_sequence)]
    sequence: u64,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Ping,
    Status,
    Sql {
        statement: String,
        #[arg(long = "param", value_parser = parse_value)]
        parameters: Vec<Value>,
        #[arg(long)]
        stale: bool,
    },
    /// Execute one or more statements in a single explicit transaction.
    Transaction {
        #[arg(long = "statement", required = true, num_args = 1..)]
        statements: Vec<String>,
    },
}

struct Client {
    stream: TcpStream,
    next_request_id: u64,
}

impl Client {
    async fn connect(address: SocketAddr) -> anyhow::Result<Self> {
        let stream = TcpStream::connect(address)
            .await
            .with_context(|| format!("connect to {address}"))?;
        Ok(Self {
            stream,
            next_request_id: 1,
        })
    }

    async fn request(
        &mut self,
        session: Option<SessionRequest>,
        operation: ClientOperation,
    ) -> anyhow::Result<ResponseResult> {
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| anyhow!("request id space exhausted"))?;
        write_message(
            &mut self.stream,
            &WireMessage::Request(ClientRequest {
                request_id,
                session,
                operation,
            }),
            DEFAULT_MAX_FRAME_BYTES,
        )
        .await?;
        let Some(WireMessage::Response(response)) =
            read_message(&mut self.stream, DEFAULT_MAX_FRAME_BYTES).await?
        else {
            bail!("server closed without a client response");
        };
        if response.request_id != request_id {
            bail!(
                "response id {} does not match request {request_id}",
                response.request_id
            );
        }
        Ok(response.result)
    }
}

fn parse_value(input: &str) -> Result<Value, String> {
    if input.eq_ignore_ascii_case("null") {
        return Ok(Value::Null);
    }
    if input.eq_ignore_ascii_case("true") {
        return Ok(Value::Bool(true));
    }
    if input.eq_ignore_ascii_case("false") {
        return Ok(Value::Bool(false));
    }
    if let Some(text) = input.strip_prefix("text:") {
        return Ok(Value::Text(text.into()));
    }
    if let Some(hex) = input.strip_prefix("hex:") {
        let mut value = Vec::with_capacity(hex.len() / 2);
        let chunks = hex.as_bytes().chunks_exact(2);
        if !chunks.remainder().is_empty() {
            return Err("hex parameter must contain pairs of digits".into());
        }
        for pair in chunks {
            let digits = std::str::from_utf8(pair).map_err(|error| error.to_string())?;
            value.push(u8::from_str_radix(digits, 16).map_err(|error| error.to_string())?);
        }
        return Ok(Value::Bytes(value));
    }
    input
        .parse::<i64>()
        .map(Value::Int64)
        .map_err(|_| "use an integer, true, false, null, text:<value>, or hex:<bytes>".into())
}

fn parse_client_id(input: &str) -> Result<[u8; 16], String> {
    if input.len() != 32 || !input.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("client id must contain exactly 32 hexadecimal digits".into());
    }
    let mut client_id = [0_u8; 16];
    for (index, byte) in client_id.iter_mut().enumerate() {
        let start = index * 2;
        *byte =
            u8::from_str_radix(&input[start..start + 2], 16).map_err(|error| error.to_string())?;
    }
    if client_id == [0; 16] {
        return Err("client id must not be all zero".into());
    }
    Ok(client_id)
}

fn parse_sequence(input: &str) -> Result<u64, String> {
    let sequence = input
        .parse::<u64>()
        .map_err(|error| format!("invalid sequence: {error}"))?;
    if sequence == 0 {
        return Err("sequence numbers start at one".into());
    }
    Ok(sequence)
}

fn new_client_id() -> [u8; 16] {
    let mut client_id: [u8; 16] = rand::random();
    if client_id == [0; 16] {
        client_id[0] = 1;
    }
    client_id
}

fn session(client_id: [u8; 16], sequence: u64, transaction_id: Option<u64>) -> SessionRequest {
    SessionRequest {
        client_id,
        sequence,
        transaction_id,
    }
}

fn sql_operation(statement: String, parameters: Vec<Value>, stale: bool) -> ClientOperation {
    ClientOperation::Execute {
        sql: statement,
        parameters,
        consistency: if stale {
            ReadConsistency::Stale
        } else {
            ReadConsistency::Linearizable
        },
    }
}

fn protocol_result(result: ResponseResult) -> anyhow::Result<ResponseResult> {
    match result {
        ResponseResult::Error(error) => Err(anyhow!("{:?}: {}", error.code, error.message)),
        result => Ok(result),
    }
}

async fn run_transaction(
    client: &mut Client,
    statements: Vec<String>,
    client_id: [u8; 16],
    sequence: u64,
) -> anyhow::Result<Vec<ResponseResult>> {
    let begin = protocol_result(
        client
            .request(
                Some(session(client_id, sequence, None)),
                ClientOperation::Begin,
            )
            .await?,
    )?;
    let ResponseResult::Transaction { transaction_id, .. } = begin else {
        bail!("BEGIN returned an unexpected response");
    };
    let mut results = Vec::with_capacity(statements.len() + 1);
    for statement in statements {
        let result = client
            .request(
                Some(session(client_id, sequence, Some(transaction_id))),
                sql_operation(statement, Vec::new(), false),
            )
            .await?;
        if let ResponseResult::Error(error) = result {
            let _ = client
                .request(
                    Some(session(client_id, sequence, Some(transaction_id))),
                    ClientOperation::Rollback,
                )
                .await;
            bail!(
                "{:?}: {}; transaction rolled back",
                error.code,
                error.message
            );
        }
        results.push(result);
    }
    results.push(protocol_result(
        client
            .request(
                Some(session(client_id, sequence, Some(transaction_id))),
                ClientOperation::Commit,
            )
            .await?,
    )?);
    Ok(results)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let arguments = Arguments::parse();
    let mut client = Client::connect(arguments.address).await?;
    let client_id = arguments.client_id.unwrap_or_else(new_client_id);
    let sequence = arguments.sequence;
    let result = match arguments.command {
        Command::Ping => protocol_result(client.request(None, ClientOperation::Ping).await?)?,
        Command::Status => protocol_result(client.request(None, ClientOperation::Status).await?)?,
        Command::Sql {
            statement,
            parameters,
            stale,
        } => protocol_result(
            client
                .request(
                    Some(session(client_id, sequence, None)),
                    sql_operation(statement, parameters, stale),
                )
                .await?,
        )?,
        Command::Transaction { statements } => {
            let results = run_transaction(&mut client, statements, client_id, sequence).await?;
            println!("{}", serde_json::to_string_pretty(&results)?);
            return Ok(());
        }
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{parse_client_id, parse_sequence, parse_value};
    use aster_core::Value;

    #[test]
    fn parameters_are_typed_without_sql_interpolation() {
        assert_eq!(parse_value("-9").unwrap(), Value::Int64(-9));
        assert_eq!(
            parse_value("text:hello").unwrap(),
            Value::Text("hello".into())
        );
        assert_eq!(parse_value("hex:00ff").unwrap(), Value::Bytes(vec![0, 255]));
        assert!(parse_value("hello").is_err());
    }

    #[test]
    fn stable_retry_identity_is_strictly_parsed() {
        assert_eq!(
            parse_client_id("00112233445566778899aabbccddeeff").unwrap(),
            [
                0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
                0xee, 0xff,
            ]
        );
        assert!(parse_client_id("0").is_err());
        assert!(parse_client_id("00000000000000000000000000000000").is_err());
        assert_eq!(parse_sequence("9").unwrap(), 9);
        assert!(parse_sequence("0").is_err());
    }
}
