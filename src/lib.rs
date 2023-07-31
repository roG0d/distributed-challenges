
use serde::{Serialize, Deserialize, de::DeserializeOwned};
use anyhow::Context;
use std::io::{BufRead, StdoutLock, Write};

// Struct Message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<Payload>  {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<Payload>,
}

// Creation of the message for a generic type of payload
impl<P> Message<P>{
    pub fn into_reply(self, id: Option<&mut usize>) -> Self {
        Self {
            src: self.dst,
            dst: self.src,
            body: Body {
                id: id.map(|id| {
                    let mid = *id;
                    *id += 1;
                    mid
            }),
                in_reply_to: self.body.id,
                payload: self.body.payload,
            },
        }

    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<Payload> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: Payload,
}


// InitPayload struct
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag="type")]
#[serde(rename_all="snake_case")]
enum InitPayload{
    Init(Init),
    InitOk,
}

// Init struct
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Init{
    pub node_id: String, 
    pub node_ids:Vec<String>,
}

//Definition of trait Node
pub trait Node<S, Payload> {

    // Init for the specific node
    fn from_init(state: S, init: Init) -> anyhow::Result<Self> 
    where 
        Self:Sized;

    // Protocol for the specific node
    fn step<'a> (&mut self, input: Message<Payload>, output: &mut StdoutLock) -> anyhow::Result<()>;
}

pub fn main_loop<S, N, P>(init_state: S) -> anyhow::Result<()> 
where 
    P: DeserializeOwned,
    N: Node<S, P>,
{

    // Lock the stdin for the thread 
    let stdin = std::io::stdin().lock();
    let mut stdin = stdin.lines();


    // Lock the stdout for the thread
    let mut stdout = std::io::stdout().lock();

    // Get the first msg from stdin to check if there's messages
    let init_msg: Message<InitPayload> = serde_json::from_str(
        &stdin.next()
        .expect("no init message received")
        .context("failed to read init message from stdin")?,
    ).context("init message could not be deserialized")?;

    // Check if the first msg it's a init_msg
    let InitPayload::Init(init) = init_msg.body.payload else {
        panic!("first message should be init");
    };
    
    // If so, initialize the node and send a initOk reply
    let mut node:N = Node::from_init(init_state, init).context("node initilization failed")?;

    let reply = Message {
        src: init_msg.dst,
        dst: init_msg.src,
        body: Body {
            id: Some(0),
            in_reply_to: init_msg.body.id,
            payload: InitPayload::InitOk,
        }
    };

    serde_json::to_writer(&mut stdout, &reply).context("serialize response to init")?;
    stdout.write_all(b"\n").context("write trailing newline")?;


    // Continues with the rest of the protocols
    for line in stdin {
        let line = line.context("Maelstrom input from STDIN could not be read")?;
        let input: Message<P> = serde_json::from_str(&line)
            .context("Maelstrom input from STDIN could not be deserialized")?;
        node.step(input, &mut stdout)
            .context("Node step function failed")?;
    }

    Ok(())
}

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10

// command to run malestron echo test, has to be on maelstrom file where maelstrom.bash is (have to indicate the rust compilation target tooz)
// ./maelstrom test -w echo --bin ../../rustengan/target/debug/rustengan --node-count 1 --time-limit 10